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
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

const (
	// 工作负载类型标签值
	WorkloadTraining  = "training"
	WorkloadInference = "inference"

	// 标签键:controller 用它们找到"属于这个 CR"的子资源与 Pod(幕3 拍板:标签选择器)
	LabelNameKey     = "elasticworkload.aiinfra.example.com/name"
	LabelWorkloadKey = "elasticworkload.aiinfra.example.com/workload"

	// finalizer:删除前保护(幕6)。子资源靠 ownerReference 级联回收;finalizer 提供
	// phase=Deleting 可见性 + 未来优雅收尾的 hook 点
	FinalizerName = "aiinfra.example.com/finalizer"
)

// workloadLabels 返回某个 CR 下某类工作负载的统一标签
func workloadLabels(crName, workload string) map[string]string {
	return map[string]string{
		LabelNameKey:     crName,
		LabelWorkloadKey: workload,
	}
}

// ElasticWorkloadReconciler reconciles a ElasticWorkload object
type ElasticWorkloadReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Metrics MetricsReader // 数据面 KV Router 的 ttft_p95 读取器(幕4b;单测注入 fake)

	// Recorder 发 K8s Events(幕6b:扩缩可见性;nil 安全,单测可不配)
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=aiinfra.example.com,resources=elasticworkloads,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=aiinfra.example.com,resources=elasticworkloads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=aiinfra.example.com,resources=elasticworkloads/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// 每次一个 ElasticWorkload 被创建/修改/删除都会触发一次;失败返回 error → 退避重试。
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
	for _, workload := range []string{WorkloadTraining, WorkloadInference} {
		if err := r.ensureDeployment(ctx, &ew, workload); err != nil {
			return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "EnsureFailed", err))
		}
	}
	if err := r.apply(ctx, &ew); err != nil {
		return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "ApplyFailed", err))
	}
	ready, err := r.reportWorkloads(ctx, &ew)
	if err != nil {
		return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "ObservationFailed", err))
	}
	if !ready {
		return ctrl.Result{RequeueAfter: tickSeconds}, nil
	}
	dec, err := r.decideStep(ctx, &ew)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Metrics unavailable; keeping committed allocation")
		return ctrl.Result{RequeueAfter: tickSeconds}, r.reportMetrics(ctx, &ew, err)
	}
	if err := r.reportMetrics(ctx, &ew, nil); err != nil {
		return ctrl.Result{}, err
	}
	if dec.Action != "none" {
		if err := r.commit(ctx, &ew, dec); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.apply(ctx, &ew); err != nil {
			return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "ApplyFailed", err))
		}
		if _, err := r.reportWorkloads(ctx, &ew); err != nil {
			return ctrl.Result{}, errors.Join(err, r.reportFailure(ctx, &ew, "ObservationFailed", err))
		}
	}
	return ctrl.Result{RequeueAfter: tickSeconds}, nil
}

// desiredDeployment 构造某个工作负载的 Deployment(仅创建时用 spec.replicas 作为初始值)
func (r *ElasticWorkloadReconciler) desiredDeployment(ew *aiinfrav1alpha1.ElasticWorkload, workload string) (*appsv1.Deployment, error) {
	var (
		name      string
		image     string
		command   []string
		replicas  int32
		gpus      int32
		resources aiinfrav1alpha1.WorkloadResources
	)
	switch workload {
	case WorkloadTraining:
		name = ew.Name + "-training"
		image = ew.Spec.Training.Image
		command = ew.Spec.Training.Command
		replicas = ew.Spec.Training.Replicas
		gpus = ew.Spec.Training.GPUPerWorker
		resources = ew.Spec.Training.Resources
	case WorkloadInference:
		name = ew.Name + "-inference"
		image = ew.Spec.Inference.Image
		command = ew.Spec.Inference.Command
		replicas = ew.Spec.Inference.Replicas
		gpus = ew.Spec.Inference.GPUPerInstance
		resources = ew.Spec.Inference.Resources
	default:
		return nil, fmt.Errorf("unknown workload: %s", workload)
	}

	if apimeta.IsStatusConditionTrue(ew.Status.Conditions, conditionInitialized) {
		if workload == WorkloadTraining {
			replicas = ew.Status.DesiredReplicas.Training
		} else {
			replicas = ew.Status.DesiredReplicas.Inference
		}
	}
	labels := workloadLabels(ew.Name, workload)
	var probe *corev1.Probe
	if workload == WorkloadInference {
		var err error
		probe, err = readinessProbe(ew.Spec.Inference.ReadinessHTTP)
		if err != nil {
			return nil, err
		}
	}
	requirements, err := workloadResources(resources, gpus)
	if err != nil {
		return nil, err
	}
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ew.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    workload,
							Image:   image,
							Command: command,
							// gpuPerWorker/GPUPerInstance=0 → 不请求 GPU(无 GPU 集群/数据面 mock 场景);
							// >0 → 请求 nvidia.com/gpu(真 GPU 集群)。L2 用 0,busybox 才能真的 Ready。
							Resources:      requirements,
							ReadinessProbe: probe,
						},
					},
				},
			},
		},
	}
	// 子资源归 CR 所有:删 CR → 级联删 Deployment(面试必答:ownerReference)
	if err := controllerutil.SetControllerReference(ew, deploy, r.Scheme); err != nil {
		return nil, err
	}
	return deploy, nil
}

// gpuLimits:gpus<=0 → 不请求 GPU(返回 nil,容器无资源限制);>0 → 请求 nvidia.com/gpu。
func gpuLimits(gpus int32) corev1.ResourceList {
	if gpus <= 0 {
		return nil
	}
	return corev1.ResourceList{
		"nvidia.com/gpu": resource.MustParse(fmt.Sprintf("%d", gpus)),
	}
}

// ensureDeployment 保证子 Deployment 存在且归 CR 所有(幂等;不动已存在的 replicas,避免与 apply 阶段打架)
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

	// 已存在 → 确认归我们管(不强行收养未标记的资源)
	if !isOwnedBy(&existing, ew) {
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

// finalize 处理删除:标记 Deleting(可见性),然后移除 finalizer 放行删除。
// 子资源由 ownerReference 级联回收,不需要这里动手;finalizer 的意义是:
//   - 删除可见性(phase=Deleting)
//   - 若将来需要优雅收尾(vLLM drain 等),这里是 hook 点
func (r *ElasticWorkloadReconciler) finalize(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) error {
	logger := logf.FromContext(ctx)
	if ew.Status.Phase != "Deleting" {
		ew.Status.Phase = "Deleting"
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

// isOwnedBy 判断 Deployment 是否被 CR 以 controller ownerReference 拥有
func isOwnedBy(obj *appsv1.Deployment, ew *aiinfrav1alpha1.ElasticWorkload) bool {
	for _, ref := range obj.OwnerReferences {
		if ref.Controller != nil && *ref.Controller && ref.UID == ew.UID {
			return true
		}
	}
	return false
}

// event 是事件记录的 nil 安全包装(单测不配 Recorder 也不 panic)
func (r *ElasticWorkloadReconciler) event(ew *aiinfrav1alpha1.ElasticWorkload, eventType, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(ew, eventType, reason, msg)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ElasticWorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("elasticworkload")
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiinfrav1alpha1.ElasticWorkload{}).
		Owns(&appsv1.Deployment{}).
		Named("elasticworkload").
		Complete(r)
}
