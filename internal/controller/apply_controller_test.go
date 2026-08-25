package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

// 全链路:ensure + observe + decide + apply。
// 过载让渡 → 训练 4→3、推理 2→3(守恒),status.scalingHistory 记 1 条审计事件。
func TestReconcile_OverloadScalesBothAndRecordsEvent(t *testing.T) {
	r, _ := newTestEnv(t, 412, nil)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}

	depT := &appsv1.Deployment{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-training", Namespace: "default"}, depT); err != nil {
		t.Fatal(err)
	}
	if *depT.Spec.Replicas != 3 {
		t.Fatalf("training want 3, got %d", *depT.Spec.Replicas)
	}
	depI := &appsv1.Deployment{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-inference", Namespace: "default"}, depI); err != nil {
		t.Fatal(err)
	}
	if *depI.Spec.Replicas != 3 {
		t.Fatalf("inference want 3, got %d", *depI.Spec.Replicas)
	}

	got := &aiinfrav1alpha1.ElasticWorkload{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.ScalingHistory) != 1 {
		t.Fatalf("want 1 scaling event, got %d", len(got.Status.ScalingHistory))
	}
	ev := got.Status.ScalingHistory[0]
	if ev.Reason != "overload" || ev.Action != "inference+1/training-1" || ev.Metric != "ttft_p95=412ms" {
		t.Fatalf("unexpected event: %+v", ev)
	}
}

// 扩缩落盘 → 发一条 K8s Event(幕6b report 层:describe 可见的操作日志)
func TestReconcile_EmitsScaleEvent(t *testing.T) {
	r, _ := newTestEnv(t, 412, nil)
	rec := record.NewFakeRecorder(10)
	r.Recorder = rec

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "Normal Scaled") || !strings.Contains(ev, "overload") {
			t.Fatalf("unexpected event: %q", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("expected a Scaled event")
	}
}

// 回归测试(幕8 声明式目标收敛的核心修复):
// 决策已提交(status.desiredReplicas=3/3,min_hold 计时生效),但 apply 部分失败
// (训练已收敛到 3、推理仍 2,模拟上次推理写冲突没成功)。下一次 Reconcile 必须
// 朝"已提交目标"收敛而非"重新叠加 delta":训练已是 3 → no-op(不再减成 2),
// 推理 2→3 补上 → 总数回到 6,不丢卡。
func TestReconcile_PartialApplyConvergesToCommittedDesired(t *testing.T) {
	s := runtime.NewScheme()
	if err := aiinfrav1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	recent := metav1.NewTime(time.Now().Add(-10 * time.Second)) // < 120s hold → min_hold 拦截重新决策
	ew := &aiinfrav1alpha1.ElasticWorkload{
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo", Namespace: "default", UID: "uid-1",
			Finalizers: []string{FinalizerName}, // 已走过首次 reconcile
		},
		Spec: aiinfrav1alpha1.ElasticWorkloadSpec{
			Training:      aiinfrav1alpha1.TrainingSpec{Replicas: 4, MinReplicas: 2, MaxReplicas: 8, GPUPerWorker: 1, Image: "train-img"},
			Inference:     aiinfrav1alpha1.InferenceSpec{Replicas: 2, MinReplicas: 1, MaxReplicas: 4, GPUPerInstance: 1, Image: "vllm-img"},
			ScalingPolicy: *policy(),
		},
		Status: aiinfrav1alpha1.ElasticWorkloadStatus{
			Phase: "Running",
			// 已提交的目标分配(训练3/推理3)= 上次决策意图,审计已记(兼作 min_hold 计时)
			DesiredReplicas: aiinfrav1alpha1.WorkloadReplicas{Training: 3, Inference: 3},
			ScalingHistory: []aiinfrav1alpha1.ScalingEvent{
				{Time: recent, Action: "inference+1/training-1", Reason: "overload"},
			},
		},
	}

	// 模拟 apply 部分失败后的集群状态:训练已收敛 3、推理仍是 2
	depT := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-training", Namespace: "default",
			Labels: workloadLabels("demo", WorkloadTraining)},
		Spec: appsv1.DeploymentSpec{Replicas: int32Ptr(3)},
	}
	depI := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-inference", Namespace: "default",
			Labels: workloadLabels("demo", WorkloadInference)},
		Spec: appsv1.DeploymentSpec{Replicas: int32Ptr(2)},
	}
	owner := true
	ref := metav1.OwnerReference{
		APIVersion: "aiinfra.example.com/v1alpha1", Kind: "ElasticWorkload",
		Name: ew.Name, UID: ew.UID, Controller: &owner,
	}
	depT.OwnerReferences = []metav1.OwnerReference{ref}
	depI.OwnerReferences = []metav1.OwnerReference{ref}

	r := &ElasticWorkloadReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(s).
			WithObjects(ew, depT, depI).
			WithStatusSubresource(&aiinfrav1alpha1.ElasticWorkload{}).
			Build(),
		Scheme:  s,
		Metrics: &fakeMetrics{ttft: 412}, // 仍过载,但 min_hold 应拦住重新决策
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}

	// 关键断言:训练仍=3(不叠加再减),推理补到 3 → 总数回到 6,不丢卡
	gotT := &appsv1.Deployment{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-training", Namespace: "default"}, gotT); err != nil {
		t.Fatal(err)
	}
	if *gotT.Spec.Replicas != 3 {
		t.Fatalf("训练不应再被减(旧版 delta 叠加会成 2), want 3 got %d", *gotT.Spec.Replicas)
	}
	gotI := &appsv1.Deployment{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-inference", Namespace: "default"}, gotI); err != nil {
		t.Fatal(err)
	}
	if *gotI.Spec.Replicas != 3 {
		t.Fatalf("推理应收敛到已提交目标 3, got %d", *gotI.Spec.Replicas)
	}

	// 收敛不重复记账(min_hold 拦截下不应再 commit)
	got := &aiinfrav1alpha1.ElasticWorkload{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.ScalingHistory) != 1 {
		t.Fatalf("min_hold 拦截后不应新增审计, got %d 条", len(got.Status.ScalingHistory))
	}
	if got.Status.DesiredReplicas.Training != 3 || got.Status.DesiredReplicas.Inference != 3 {
		t.Fatalf("desired 应保持已提交值(3,3), got %+v", got.Status.DesiredReplicas)
	}
}

// 死区不动作:replicas 不变、不记录事件(无谓 API 写归零)
func TestReconcile_DeadZoneNoOp(t *testing.T) {
	r, _ := newTestEnv(t, 210, nil)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}

	depT := &appsv1.Deployment{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-training", Namespace: "default"}, depT); err != nil {
		t.Fatal(err)
	}
	if *depT.Spec.Replicas != 4 {
		t.Fatalf("training 不应变, got %d", *depT.Spec.Replicas)
	}
	depI := &appsv1.Deployment{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-inference", Namespace: "default"}, depI); err != nil {
		t.Fatal(err)
	}
	if *depI.Spec.Replicas != 2 {
		t.Fatalf("inference 不应变, got %d", *depI.Spec.Replicas)
	}

	got := &aiinfrav1alpha1.ElasticWorkload{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.ScalingHistory) != 0 {
		t.Fatalf("死区不应记录事件, got %d", len(got.Status.ScalingHistory))
	}
}
