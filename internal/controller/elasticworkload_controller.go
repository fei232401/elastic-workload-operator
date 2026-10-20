/*
Copyright 2026 fei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

const (
	WorkloadTraining  = "training"
	WorkloadInference = "inference"

	LabelNameKey     = "elasticworkload.aiinfra.example.com/name"
	LabelWorkloadKey = "elasticworkload.aiinfra.example.com/workload"

	FinalizerName = "aiinfra.example.com/finalizer"
)

func workloadLabels(crName, workload string) map[string]string {
	return map[string]string{
		LabelNameKey:     crName,
		LabelWorkloadKey: workload,
	}
}

// ElasticWorkloadReconciler reconciles a ElasticWorkload object
type ElasticWorkloadReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Metrics     MetricsReader
	Checkpoints CheckpointClient

	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=aiinfra.example.com,resources=elasticworkloads,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=aiinfra.example.com,resources=elasticworkloads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=aiinfra.example.com,resources=elasticworkloads/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *ElasticWorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ew aiinfrav1alpha1.ElasticWorkload
	if err := r.Get(ctx, req.NamespacedName, &ew); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !ew.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, &ew)
	}
	if !controllerutil.ContainsFinalizer(&ew, FinalizerName) {
		controllerutil.AddFinalizer(&ew, FinalizerName)
		if err := r.Update(ctx, &ew); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.initializeAllocation(ctx, &ew); err != nil {
		return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "InitializationFailed", err))
	}
	if err := validateAllocation(&ew); err != nil {
		return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "InvalidAllocation", err))
	}
	if err := r.ensureTraining(ctx, &ew); err != nil {
		return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "EnsureFailed", err))
	}
	if err := r.ensureDeployment(ctx, &ew, WorkloadInference); err != nil {
		return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "EnsureFailed", err))
	}
	if ew.Status.Handoff != nil {
		return r.reconcileHandoff(ctx, &ew)
	}
	if err := r.apply(ctx, &ew); err != nil {
		return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "ApplyFailed", err))
	}
	ready, err := r.reportWorkloads(ctx, &ew)
	if err != nil {
		return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "ObservationFailed", err))
	}
	if !ready {
		return ctrl.Result{RequeueAfter: tickInterval}, nil
	}
	dec, err := r.decideStep(ctx, &ew)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Metrics unavailable; keeping committed allocation")
		return ctrl.Result{RequeueAfter: tickInterval}, r.reportMetrics(ctx, &ew, err)
	}
	if err := r.reportMetrics(ctx, &ew, nil); err != nil {
		return ctrl.Result{}, err
	}
	if dec.Action != actionNone {
		if err := r.commit(ctx, &ew, dec); err != nil {
			return ctrl.Result{}, err
		}
		if ew.Status.Handoff != nil {
			return r.reconcileHandoff(ctx, &ew)
		}
		if err := r.apply(ctx, &ew); err != nil {
			return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "ApplyFailed", err))
		}
		if _, err := r.reportWorkloads(ctx, &ew); err != nil {
			return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "ObservationFailed", err))
		}
	}
	return ctrl.Result{RequeueAfter: tickInterval}, nil
}

func initialReplicas(ew *aiinfrav1alpha1.ElasticWorkload, workload string) int32 {
	initialized := apimeta.IsStatusConditionTrue(ew.Status.Conditions, conditionInitialized)
	effective := effectiveTargets(ew)
	switch {
	case workload == WorkloadTraining && initialized:
		return effective.Training
	case workload == WorkloadTraining:
		return ew.Spec.Training.Replicas
	case initialized:
		return effective.Inference
	default:
		return ew.Spec.Inference.Replicas
	}
}

func managedContainer(ew *aiinfrav1alpha1.ElasticWorkload, workload string) (corev1.Container, error) {
	container := corev1.Container{Name: workload}
	var (
		gpus      int32
		resources aiinfrav1alpha1.WorkloadResources
		err       error
	)
	switch workload {
	case WorkloadTraining:
		container.Image, container.Command = ew.Spec.Training.Image, ew.Spec.Training.Command
		gpus, resources = ew.Spec.Training.GPUPerWorker, ew.Spec.Training.Resources
		container.ReadinessProbe = trainingProbe(ew)
		container.Env = trainingEnv(ew)
	case WorkloadInference:
		container.Image, container.Command = ew.Spec.Inference.Image, ew.Spec.Inference.Command
		gpus, resources = ew.Spec.Inference.GPUPerInstance, ew.Spec.Inference.Resources
		if container.ReadinessProbe, err = readinessProbe(ew.Spec.Inference.ReadinessHTTP); err != nil {
			return container, err
		}
	default:
		return container, fmt.Errorf("unknown workload: %s", workload)
	}
	container.Resources, err = workloadResources(resources, gpus)
	return container, err
}

func (r *ElasticWorkloadReconciler) desiredDeployment(ew *aiinfrav1alpha1.ElasticWorkload, workload string) (*appsv1.Deployment, error) {
	container, err := managedContainer(ew, workload)
	if err != nil {
		return nil, err
	}
	replicas := initialReplicas(ew, workload)
	labels := workloadLabels(ew.Name, workload)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: childName(ew, workload), Namespace: ew.Namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{container}},
			},
		},
	}
	if err := controllerutil.SetControllerReference(ew, deploy, r.Scheme); err != nil {
		return nil, err
	}
	return deploy, nil
}

func gpuLimits(gpus int32) corev1.ResourceList {
	if gpus <= 0 {
		return nil
	}
	return corev1.ResourceList{
		"nvidia.com/gpu": resource.MustParse(fmt.Sprintf("%d", gpus)),
	}
}

func (r *ElasticWorkloadReconciler) ensureDeployment(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload, workload string) error {
	logger := logf.FromContext(ctx)
	desired, err := r.desiredDeployment(ew, workload)
	if err != nil {
		return err
	}

	var existing appsv1.Deployment
	err = r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: ew.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return err
		}
		logger.Info("created deployment", "name", desired.Name, "replicas", *desired.Spec.Replicas)
		return nil
	}
	if err != nil {
		return err
	}

	if !ownedBy(&existing, ew) {
		return fmt.Errorf("deployment %s/%s exists but is not owned by this ElasticWorkload; refusing to adopt",
			ew.Namespace, desired.Name)
	}
	wanted := desired.Spec.Template.Spec.Containers[0]
	for index := range existing.Spec.Template.Spec.Containers {
		container := &existing.Spec.Template.Spec.Containers[index]
		if container.Name != workload {
			continue
		}
		if container.Image == wanted.Image && equality.Semantic.DeepEqual(container.Command, wanted.Command) &&
			equality.Semantic.DeepEqual(container.Resources, wanted.Resources) && equality.Semantic.DeepEqual(container.ReadinessProbe, wanted.ReadinessProbe) {
			return nil
		}
		container.Image = wanted.Image
		container.Command = wanted.Command
		container.Resources = wanted.Resources
		container.ReadinessProbe = wanted.ReadinessProbe
		return r.Update(ctx, &existing)
	}
	return fmt.Errorf("owned deployment %s has no managed container %s", existing.Name, workload)
}

func (r *ElasticWorkloadReconciler) finalize(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) error {
	logger := logf.FromContext(ctx)
	if ew.Status.Phase != phaseDeleting {
		ew.Status.Phase = phaseDeleting
		if err := r.Status().Update(ctx, ew); err != nil {
			return err
		}
	}
	if controllerutil.ContainsFinalizer(ew, FinalizerName) {
		controllerutil.RemoveFinalizer(ew, FinalizerName)
		if err := r.Update(ctx, ew); err != nil {
			return err
		}
		logger.Info("removed finalizer, CR will be deleted")
	}
	return nil
}

func (r *ElasticWorkloadReconciler) event(ew *aiinfrav1alpha1.ElasticWorkload, eventType, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(ew, nil, eventType, reason, "Reconcile", "%s", msg)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ElasticWorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorder("elasticworkload")
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiinfrav1alpha1.ElasticWorkload{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Named("elasticworkload").
		Complete(r)
}
