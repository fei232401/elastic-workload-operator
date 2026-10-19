package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

func (r *ElasticWorkloadReconciler) commit(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload, dec ScalingDecision) error {
	logger := logf.FromContext(ctx)
	ew.Status.DesiredReplicas = aiinfrav1alpha1.WorkloadReplicas{
		Training:  dec.DesiredTraining,
		Inference: dec.DesiredInference,
	}
	ew.Status.ScalingHistory = append(ew.Status.ScalingHistory, aiinfrav1alpha1.ScalingEvent{
		Time:   metav1.Now(),
		Action: fmt.Sprintf("inference%+d/training%+d", dec.InferenceDelta, dec.TrainingDelta),
		Reason: decisionReason(dec),
		Metric: fmt.Sprintf("ttft_p95=%dms", dec.TTFTMS),
	})
	if n := len(ew.Status.ScalingHistory); n > scalingHistoryLimit {
		ew.Status.ScalingHistory = append([]aiinfrav1alpha1.ScalingEvent(nil), ew.Status.ScalingHistory[n-scalingHistoryLimit:]...)
	}
	ew.Status.Phase = "Progressing"
	apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionReady, metav1.ConditionFalse, "AllocationPending", "Committed target is awaiting workload convergence"))
	apimeta.SetStatusCondition(&ew.Status.Conditions, newCondition(ew, conditionProgressing, metav1.ConditionTrue, "AllocationPending", "Applying committed replica targets"))
	if err := r.Status().Update(ctx, ew); err != nil {
		return err
	}
	logger.Info("committed", "desiredTraining", dec.DesiredTraining, "desiredInference", dec.DesiredInference, "reason", decisionReason(dec))
	r.event(ew, corev1.EventTypeNormal, "DecisionCommitted",
		fmt.Sprintf("inference=%d/training=%d (%s, ttft_p95=%dms)",
			dec.DesiredInference, dec.DesiredTraining, decisionReason(dec), dec.TTFTMS))
	return nil
}

func (r *ElasticWorkloadReconciler) apply(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) error {
	logger := logf.FromContext(ctx)
	desired := ew.Status.DesiredReplicas

	for _, u := range []struct {
		workload string
		target   int32
	}{
		{WorkloadTraining, desired.Training},
		{WorkloadInference, desired.Inference},
	} {
		var dep appsv1.Deployment
		if err := r.Get(ctx, types.NamespacedName{Name: ew.Name + "-" + u.workload, Namespace: ew.Namespace}, &dep); err != nil {
			return err
		}
		if !isOwnedBy(&dep, ew) {
			return fmt.Errorf("deployment %s/%s is not owned by this ElasticWorkload", dep.Namespace, dep.Name)
		}
		cur := int32(0)
		if dep.Spec.Replicas != nil {
			cur = *dep.Spec.Replicas
		}
		if cur == u.target {
			continue
		}
		target := u.target
		dep.Spec.Replicas = &target
		if err := r.Update(ctx, &dep); err != nil {
			return err
		}
		logger.Info("applied", "workload", u.workload, "replicas", target)
	}
	return nil
}

func decisionReason(dec ScalingDecision) string {
	switch dec.Action {
	case "scale_inference_up":
		return "overload"
	case "scale_inference_down":
		return "recovery"
	}
	return "none"
}
