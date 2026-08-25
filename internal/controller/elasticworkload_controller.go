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
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// 每次一个 ElasticWorkload 被创建/修改/删除都会触发一次;失败返回 error → 退避重试。
func (r *ElasticWorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	// ① 获取触发的 ElasticWorkload 实例(幂等:已删除则静默返回)
	var ew aiinfrav1alpha1.ElasticWorkload
	if err := r.Get(ctx, req.NamespacedName, &ew); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	logger.Info("reconciling", "phase", ew.Status.Phase)

	// 删除路径(幕6):子资源靠 ownerReference 级联回收;标记 Deleting → 移除 finalizer 放行删除
	if !ew.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, &ew)
	}

	// 首次接入:加 finalizer(此后删除会被拦下先走 finalize),并置 Pending
	if !controllerutil.ContainsFinalizer(&ew, FinalizerName) {
		controllerutil.AddFinalizer(&ew, FinalizerName)
		if err := r.Update(ctx, &ew); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("added finalizer", "finalizer", FinalizerName)
		if ew.Status.Phase == "" {
			ew.Status.Phase = "Pending"
			// 初始化声明式目标分配 = spec 初值(幕8:apply 只朝 status.desiredReplicas 收敛,
			// 未初始化会误把 Deployment 收敛到 0)
			ew.Status.DesiredReplicas = aiinfrav1alpha1.WorkloadReplicas{
				Training:  ew.Spec.Training.Replicas,
				Inference: ew.Spec.Inference.Replicas,
			}
			if err := r.Status().Update(ctx, &ew); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	// ② ensure 子资源:训练/推理 Deployment(不存在则创建,带 ownerReference)
	for _, wl := range []string{WorkloadTraining, WorkloadInference} {
		if err := r.ensureDeployment(ctx, &ew, wl); err != nil {
			return ctrl.Result{}, err
		}
	}

	// ③ observe:读两个子 Deployment 实际 ready 数,写回 status.readyReplicas(仅变化时更新)
	if err := r.observe(ctx, &ew); err != nil {
		return ctrl.Result{}, err
	}
	// 报告 phase(幕6):孩子们在管 = Running(仅当变化时写,避免循环)
	if ew.Status.Phase != "Running" {
		ew.Status.Phase = "Running"
		if err := r.Status().Update(ctx, &ew); err != nil {
			return ctrl.Result{}, err
		}
	}
	// ④ decide(幕4):读 ttft_p95 + min_hold 闸门 → Rule1-5
	dec, err := r.decideStep(ctx, &ew)
	if err != nil {
		return ctrl.Result{}, err
	}
	// ⑤ commit(幕8 修复):非 none 才把"目标分配 + 审计"原子写进 status.desiredReplicas
	if dec.Action != "none" {
		if err := r.commit(ctx, &ew, dec); err != nil {
			return ctrl.Result{}, err
		}
	}
	// ⑥ apply(幕5/幕8):朝 status.desiredReplicas 收敛(幂等,变了才写,部分失败重试不叠加)
	if err := r.apply(ctx, &ew); err != nil {
		return ctrl.Result{}, err
	}
	// ⑦ finalizer + report(幕6):删除回收 + status/Event

	// 定时醒来(幕6d):项目一 tick=5s 模型——即使无事件也周期性重决策。
	// 没有它,扩一次后无事件触发就歇了;min_hold 在 decideStep 内闸门,不会 5s 乱动
	return ctrl.Result{RequeueAfter: tickSeconds}, nil
}

// desiredDeployment 构造某个工作负载的 Deployment(仅创建时用 spec.replicas 作为初始值)
func (r *ElasticWorkloadReconciler) desiredDeployment(ew *aiinfrav1alpha1.ElasticWorkload, workload string) (*appsv1.Deployment, error) {
	var (
		name     string
		image    string
		command  []string
		replicas int32
		gpus     int32
	)
	switch workload {
	case WorkloadTraining:
		name = ew.Name + "-training"
		image = ew.Spec.Training.Image
		command = ew.Spec.Training.Command
		replicas = ew.Spec.Training.Replicas
		gpus = ew.Spec.Training.GPUPerWorker
	case WorkloadInference:
		name = ew.Name + "-inference"
		image = ew.Spec.Inference.Image
		command = ew.Spec.Inference.Command
		replicas = ew.Spec.Inference.Replicas
		gpus = ew.Spec.Inference.GPUPerInstance
	default:
		return nil, fmt.Errorf("unknown workload: %s", workload)
	}

	labels := workloadLabels(ew.Name, workload)
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
							Resources: corev1.ResourceRequirements{Limits: gpuLimits(gpus)},
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
	return nil
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

// observe 读取两个子 Deployment 的 ready replicas,写入 status.readyReplicas。
// 只在值变化时才写回(status 更新会触发新 reconcile;无脑写 = 无限循环,面试常问)。
func (r *ElasticWorkloadReconciler) observe(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) error {
	ready, err := r.readyReplicas(ctx, ew)
	if err != nil {
		return err
	}
	if ew.Status.ReadyReplicas.Training != ready.Training || ew.Status.ReadyReplicas.Inference != ready.Inference {
		ew.Status.ReadyReplicas = ready
		if err := r.Status().Update(ctx, ew); err != nil {
			return err
		}
	}
	return nil
}

// readyReplicas 按标签列出属于本 CR 的两个子 Deployment,累加其 ReadyReplicas
func (r *ElasticWorkloadReconciler) readyReplicas(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) (aiinfrav1alpha1.WorkloadReplicas, error) {
	var out aiinfrav1alpha1.WorkloadReplicas
	for _, wl := range []string{WorkloadTraining, WorkloadInference} {
		var deploys appsv1.DeploymentList
		if err := r.List(ctx, &deploys,
			client.InNamespace(ew.Namespace),
			client.MatchingLabels(workloadLabels(ew.Name, wl)),
		); err != nil {
			return out, err
		}
		var n int32
		for i := range deploys.Items {
			n += deploys.Items[i].Status.ReadyReplicas
		}
		switch wl {
		case WorkloadTraining:
			out.Training = n
		case WorkloadInference:
			out.Inference = n
		}
	}
	return out, nil
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
		Named("elasticworkload").
		Complete(r)
}
