package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

type fakeMetrics struct {
	ttft   int32
	err    error
	source aiinfrav1alpha1.MetricSource
}

func (f *fakeMetrics) TTFTP95MS(_ context.Context, source aiinfrav1alpha1.MetricSource) (int32, error) {
	f.source = source
	return f.ttft, f.err
}

func int32Ptr(n int32) *int32 { return &n }

// newTestEnv 构造:CR(训练分配4/推理分配2,min/max 同 scaling_test) + 两个子 Deployment + fake 指标
func newTestEnv(t *testing.T, ttft int32, lastScale *metav1.Time) (*ElasticWorkloadReconciler, *aiinfrav1alpha1.ElasticWorkload) {
	t.Helper()
	s := runtime.NewScheme()
	if err := aiinfrav1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	ew := &aiinfrav1alpha1.ElasticWorkload{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "uid-1"},
		Spec: aiinfrav1alpha1.ElasticWorkloadSpec{
			Training: aiinfrav1alpha1.TrainingSpec{
				Replicas: 4, MinReplicas: 2, MaxReplicas: 8, GPUPerWorker: 1, Image: "train-img"},
			Inference: aiinfrav1alpha1.InferenceSpec{
				Replicas: 2, MinReplicas: 1, MaxReplicas: 4, GPUPerInstance: 1, Image: "vllm-img"},
			ScalingPolicy: *policy(),
		},
	}
	ew.Status.DesiredReplicas = aiinfrav1alpha1.WorkloadReplicas{Training: 4, Inference: 2}
	if lastScale != nil {
		ew.Status.ScalingHistory = []aiinfrav1alpha1.ScalingEvent{
			{Time: *lastScale, Action: "inference+1/training-1", Reason: "overload"},
		}
	}

	depI := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-inference", Namespace: "default",
			Labels: workloadLabels("demo", WorkloadInference)},
		Spec:   appsv1.DeploymentSpec{Replicas: int32Ptr(2)},
		Status: appsv1.DeploymentStatus{Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2},
	}
	desired, err := (&ElasticWorkloadReconciler{Scheme: s}).desiredDeployment(ew, WorkloadInference)
	if err != nil {
		t.Fatal(err)
	}
	depI.Spec.Template = desired.Spec.Template
	depT := trainingSet(t, s, ew, 4, true)
	owner := true
	ref := metav1.OwnerReference{
		APIVersion: "aiinfra.example.com/v1alpha1",
		Kind:       "ElasticWorkload",
		Name:       ew.Name,
		UID:        ew.UID,
		Controller: &owner,
	}
	depI.OwnerReferences = []metav1.OwnerReference{ref}

	// WithStatusSubresource:fake client 只对注册过 status 子资源的 GVK 放行 Status().Update,
	// 否则直接返回 NotFound(镜像真实 API server 的 CRD subresources 声明)
	r := &ElasticWorkloadReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(s).
			WithObjects(ew, depT, depI).
			WithStatusSubresource(&aiinfrav1alpha1.ElasticWorkload{}, &appsv1.StatefulSet{}, &appsv1.Deployment{}, &corev1.Pod{}).
			Build(),
		Scheme:  s,
		Metrics: &fakeMetrics{ttft: ttft},
	}
	return r, ew
}

func TestDecideStep_Overload(t *testing.T) {
	// ttft=412 > SLO=300,分配(训练4/推理2)未抵界 → 让渡:推理+1 训练-1,目标(3,3)
	r, ew := newTestEnv(t, 412, nil)
	dec, err := r.decideStep(context.Background(), ew)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Action != "scale_inference_up" || dec.InferenceDelta != 1 || dec.TrainingDelta != -1 ||
		dec.DesiredTraining != 3 || dec.DesiredInference != 3 {
		t.Fatalf("want 让渡 +1/-1 目标(3,3), got %+v", dec)
	}
}

func TestDecideStep_DeadZone(t *testing.T) {
	// ttft=210 ∈ 死区 → 不动
	r, ew := newTestEnv(t, 210, nil)
	dec, err := r.decideStep(context.Background(), ew)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Action != "none" {
		t.Fatalf("want none, got %+v", dec)
	}
}

func TestDecideStep_MinHoldGates(t *testing.T) {
	// 上次扩缩在 10s 前(< 120s hold)→ 即便过载也拦截
	recent := metav1.NewTime(time.Now().Add(-10 * time.Second))
	r, ew := newTestEnv(t, 412, &recent)
	dec, err := r.decideStep(context.Background(), ew)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Action != "none" {
		t.Fatalf("min_hold 应拦截, got %+v", dec)
	}
}

func TestDecideStep_MinHoldExpired(t *testing.T) {
	// 上次扩缩在 5 分钟前(> 120s hold)→ 闸门放行,重新让渡
	old := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	r, ew := newTestEnv(t, 412, &old)
	dec, err := r.decideStep(context.Background(), ew)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Action != "scale_inference_up" {
		t.Fatalf("min_hold 已过期应放行, got %+v", dec)
	}
}

func TestDecideStep_UsesWorkloadMetricSource(t *testing.T) {
	r, ew := newTestEnv(t, 412, nil)
	ew.Spec.ScalingPolicy.Metrics = &aiinfrav1alpha1.MetricSource{RouterURL: "http://router-b:8080", Model: "qwen-3b"}
	if _, err := r.decideStep(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	got := r.Metrics.(*fakeMetrics).source
	if got.RouterURL != "http://router-b:8080" || got.Model != "qwen-3b" {
		t.Fatalf("metric source not taken from the workload: %+v", got)
	}
}

func trainingSet(t *testing.T, s *runtime.Scheme, ew *aiinfrav1alpha1.ElasticWorkload, replicas int32, available bool) *appsv1.StatefulSet {
	t.Helper()
	set, _, err := (&ElasticWorkloadReconciler{Scheme: s}).desiredTraining(ew)
	if err != nil {
		t.Fatal(err)
	}
	set.Spec.Replicas = int32Ptr(replicas)
	if available {
		set.Status = appsv1.StatefulSetStatus{Replicas: replicas, UpdatedReplicas: replicas, ReadyReplicas: replicas, AvailableReplicas: replicas}
	}
	return set
}

func replicasOf(t *testing.T, r *ElasticWorkloadReconciler, workload string) int32 {
	t.Helper()
	ew := &aiinfrav1alpha1.ElasticWorkload{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "default"}, ew); err != nil {
		t.Fatal(err)
	}
	child, err := r.getChild(context.Background(), ew, workload)
	if err != nil {
		t.Fatal(err)
	}
	return *childReplicas(child)
}
