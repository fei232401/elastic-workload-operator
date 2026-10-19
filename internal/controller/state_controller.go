package controller

import (
	"context"
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

const (
	conditionInitialized  = "Initialized"
	conditionReady        = "Ready"
	conditionProgressing  = "Progressing"
	conditionMetricsReady = "MetricsReady"
	scalingHistoryLimit   = 50
)

func newCondition(ew *aiinfrav1alpha1.ElasticWorkload, kind string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:               kind,
		Status:             status,
		ObservedGeneration: ew.Generation,
		Reason:             reason,
		Message:            message,
	}
}

func (r *ElasticWorkloadReconciler) initializeAllocation(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) error {
	if apimeta.IsStatusConditionTrue(ew.Status.Conditions, conditionInitialized) {
		return nil
	}
	previous := ew.DeepCopy()
	initialized := apimeta.FindStatusCondition(ew.Status.Conditions, conditionInitialized)
	legacy := initialized == nil &&
		(ew.Status.Phase != "" || len(ew.Status.ScalingHistory) > 0 ||
			ew.Status.DesiredReplicas != (aiinfrav1alpha1.WorkloadReplicas{}))
	legacy = legacy || initialized != nil && initialized.Reason == "LegacyAllocationInvalid"
	reason := "InitialAllocationSaved"
	if !legacy {
		ew.Status.DesiredReplicas = aiinfrav1alpha1.WorkloadReplicas{
			Training:  ew.Spec.Training.Replicas,
			Inference: ew.Spec.Inference.Replicas,
		}
	} else {
		reason = "LegacyAllocationAdopted"
	}
	if err := validateAllocation(ew); err != nil {
		ew.Status = previous.Status
		if legacy {
			apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionInitialized, metav1.ConditionFalse,
				"LegacyAllocationInvalid", "Existing committed allocation is invalid; retaining it for explicit correction"))
		}
		return err
	}
	if ew.Status.Phase == "" {
		ew.Status.Phase = "Pending"
	}
	apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionInitialized, metav1.ConditionTrue,
		reason, "Committed replica targets are initialized"))
	return r.saveStatus(ctx, ew, previous)
}

func validateAllocation(ew *aiinfrav1alpha1.ElasticWorkload) error {
	if _, err := readinessProbe(ew.Spec.Inference.ReadinessHTTP); err != nil {
		return err
	}
	if ew.Spec.Inference.PrefixCaching {
		return fmt.Errorf("prefixCaching is not a managed engine option; configure it explicitly through the inference command")
	}
	if ew.Spec.Training.GPUPerWorker != ew.Spec.Inference.GPUPerInstance {
		return fmt.Errorf("only equal GPU units per replica are supported; CPU-only mode requires both GPU units to be zero")
	}
	for _, resources := range []aiinfrav1alpha1.WorkloadResources{ew.Spec.Training.Resources, ew.Spec.Inference.Resources} {
		if _, err := workloadResources(resources, 0); err != nil {
			return err
		}
	}
	trainingResources, _ := workloadResources(ew.Spec.Training.Resources, 0)
	inferenceResources, _ := workloadResources(ew.Spec.Inference.Resources, 0)
	if !equality.Semantic.DeepEqual(trainingResources.Requests, inferenceResources.Requests) {
		return fmt.Errorf("only equal CPU and memory requests per replica are supported")
	}
	for _, allocation := range []struct {
		name                                     string
		initial, desired, minimum, maximum, gpus int32
	}{
		{WorkloadTraining, ew.Spec.Training.Replicas, ew.Status.DesiredReplicas.Training,
			ew.Spec.Training.MinReplicas, ew.Spec.Training.MaxReplicas, ew.Spec.Training.GPUPerWorker},
		{WorkloadInference, ew.Spec.Inference.Replicas, ew.Status.DesiredReplicas.Inference,
			ew.Spec.Inference.MinReplicas, ew.Spec.Inference.MaxReplicas, ew.Spec.Inference.GPUPerInstance},
	} {
		if allocation.minimum < 0 || allocation.maximum < allocation.minimum || allocation.gpus < 0 ||
			allocation.initial < allocation.minimum || allocation.initial > allocation.maximum ||
			allocation.desired < allocation.minimum || allocation.desired > allocation.maximum {
			return fmt.Errorf("%s initial and committed replicas must be within non-negative bounds; GPU count must be non-negative", allocation.name)
		}
	}
	policy := ew.Spec.ScalingPolicy
	initialTotal := int64(ew.Spec.Training.Replicas) + int64(ew.Spec.Inference.Replicas)
	desiredTotal := int64(ew.Status.DesiredReplicas.Training) + int64(ew.Status.DesiredReplicas.Inference)
	if initialTotal != desiredTotal {
		return fmt.Errorf("committed replica total must match the declared initial total; explicit replanning is required")
	}
	if policy.SLO <= 0 || policy.MaxScaleStep < 1 || policy.MinHoldDuration < 0 {
		return fmt.Errorf("scaling policy requires positive SLO and step, and non-negative hold duration")
	}
	return nil
}

func deploymentAvailable(dep *appsv1.Deployment, target int32) bool {
	return dep.Spec.Replicas != nil && *dep.Spec.Replicas == target &&
		dep.Status.ObservedGeneration >= dep.Generation &&
		dep.Status.Replicas == target && dep.Status.UpdatedReplicas == target &&
		dep.Status.ReadyReplicas == target && dep.Status.AvailableReplicas == target &&
		dep.Status.UnavailableReplicas == 0
}

func (r *ElasticWorkloadReconciler) inspectWorkloads(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) (aiinfrav1alpha1.WorkloadReplicas, bool, string, error) {
	var replicas aiinfrav1alpha1.WorkloadReplicas
	ready := true
	reason := "WorkloadsPending"
	for _, workload := range []struct {
		name   string
		target int32
		count  *int32
	}{
		{WorkloadTraining, ew.Status.DesiredReplicas.Training, &replicas.Training},
		{WorkloadInference, ew.Status.DesiredReplicas.Inference, &replicas.Inference},
	} {
		var dep appsv1.Deployment
		if err := r.Get(ctx, types.NamespacedName{Name: ew.Name + "-" + workload.name, Namespace: ew.Namespace}, &dep); err != nil {
			return replicas, false, "ObservationFailed", err
		}
		if !isOwnedBy(&dep, ew) {
			return replicas, false, "OwnershipConflict", fmt.Errorf("deployment %s is not owned by this ElasticWorkload", dep.Name)
		}
		*workload.count = dep.Status.ReadyReplicas
		ready = ready && deploymentAvailable(&dep, workload.target)
		for _, condition := range dep.Status.Conditions {
			if condition.Type == appsv1.DeploymentProgressing && condition.Status == corev1.ConditionFalse &&
				condition.Reason == "ProgressDeadlineExceeded" {
				reason = "DeploymentProgressDeadlineExceeded"
			}
		}
	}
	return replicas, ready, reason, nil
}

func (r *ElasticWorkloadReconciler) reportWorkloads(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) (bool, error) {
	replicas, ready, reason, err := r.inspectWorkloads(ctx, ew)
	if err != nil {
		return false, err
	}
	previous := ew.DeepCopy()
	ew.Status.ReadyReplicas = replicas
	status, progressing := metav1.ConditionFalse, metav1.ConditionTrue
	message := "Waiting for Deployments to observe their generation and reach committed replica targets"
	ew.Status.Phase = "Progressing"
	if ready {
		status, progressing = metav1.ConditionTrue, metav1.ConditionFalse
		reason, message = "ReplicaTargetsAvailable", "Both Deployments have reached committed replica targets"
		ew.Status.Phase = "Running"
	} else {
		if reason == "DeploymentProgressDeadlineExceeded" {
			ew.Status.Phase = "Degraded"
			progressing = metav1.ConditionFalse
		}
		apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionMetricsReady, metav1.ConditionUnknown,
			"WaitingForWorkloads", "New metric-driven decisions are suspended until workloads converge"))
	}
	apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionReady, status, reason, message))
	apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionProgressing, progressing, reason, message))
	if err := r.saveStatus(ctx, ew, previous); err != nil {
		return false, err
	}
	if ready && !apimeta.IsStatusConditionTrue(previous.Status.Conditions, conditionReady) {
		r.event(ew, corev1.EventTypeNormal, "AllocationConverged", message)
	}
	return ready, nil
}

func (r *ElasticWorkloadReconciler) reportMetrics(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload, cause error) error {
	previous := ew.DeepCopy()
	status, reason := metav1.ConditionTrue, "MetricReadSucceeded"
	message := "Metric was read and validated for this decision cycle"
	if cause != nil {
		status, reason = metav1.ConditionFalse, "MetricsUnavailable"
		var validation *metricValidationError
		if errors.As(cause, &validation) {
			reason = validation.reason
		}
		message = "Retaining committed targets: " + cause.Error()
		if len(message) > 512 {
			message = "Metric is unavailable or invalid; retaining committed targets; consult controller logs"
		}
	}
	apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionMetricsReady, status, reason, message))
	return r.saveStatus(ctx, ew, previous)
}

func (r *ElasticWorkloadReconciler) reportFailure(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload, reason string, cause error) error {
	previous := ew.DeepCopy()
	message := cause.Error()
	if len(message) > 512 {
		message = "Controller could not complete the operation; consult controller logs"
	}
	ew.Status.Phase = "Degraded"
	if apimeta.FindStatusCondition(ew.Status.Conditions, conditionInitialized) == nil {
		apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionInitialized, metav1.ConditionFalse, reason, message))
	}
	apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionReady, metav1.ConditionFalse, reason, message))
	apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionProgressing, metav1.ConditionFalse, reason, message))
	return r.saveStatus(ctx, ew, previous)
}

func (r *ElasticWorkloadReconciler) saveStatus(ctx context.Context, ew, previous *aiinfrav1alpha1.ElasticWorkload) error {
	if equality.Semantic.DeepEqual(ew.Status, previous.Status) {
		return nil
	}
	return r.Status().Update(ctx, ew)
}
