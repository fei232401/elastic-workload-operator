package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

// 正常路径:首次 reconcile 加 finalizer,孩子们就位后 phase=Running
func TestReconcile_AddsFinalizerAndPhaseRunning(t *testing.T) {
	r, _ := newTestEnv(t, 210, nil) // 死区,不触发扩缩,聚焦 finalizer/phase
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}

	got := &aiinfrav1alpha1.ElasticWorkload{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if !contains(got.Finalizers, FinalizerName) {
		t.Fatalf("want finalizer %s, got %v", FinalizerName, got.Finalizers)
	}
	if got.Status.Phase != "Running" {
		t.Fatalf("want phase Running, got %q", got.Status.Phase)
	}
}

// 删除路径:CR 带 deletionTimestamp + finalizer → finalize 移除 finalizer → CR 真的被删
func TestReconcile_DeletionRemovesFinalizerAndDeletes(t *testing.T) {
	r := newDeletingEnv(t)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}

	err := r.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "default"}, &aiinfrav1alpha1.ElasticWorkload{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("CR 应已被删除, err=%v", err)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// 模拟 kubectl delete:CR 已带 deletionTimestamp + finalizer。
// 经 WithObjects 直接进 tracker,绕开 Update 的 "deletionTimestamp 不可变" 校验
// (fake client 的 Delete 不模拟 finalizer 拦删除,这是最接近真实语义的模拟方式)。
func newDeletingEnv(t *testing.T) *ElasticWorkloadReconciler {
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

	now := metav1.Now()
	ew := &aiinfrav1alpha1.ElasticWorkload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "demo",
			Namespace:         "default",
			UID:               "uid-1",
			DeletionTimestamp: &now,
			Finalizers:        []string{FinalizerName},
		},
		Spec: aiinfrav1alpha1.ElasticWorkloadSpec{
			Training: aiinfrav1alpha1.TrainingSpec{
				Replicas: 4, MinReplicas: 2, MaxReplicas: 8, GPUPerWorker: 1, Image: "train-img"},
			Inference: aiinfrav1alpha1.InferenceSpec{
				Replicas: 2, MinReplicas: 1, MaxReplicas: 4, GPUPerInstance: 1, Image: "vllm-img"},
			ScalingPolicy: *policy(),
		},
	}
	return &ElasticWorkloadReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(s).
			WithObjects(ew).
			WithStatusSubresource(&aiinfrav1alpha1.ElasticWorkload{}).
			Build(),
		Scheme: s,
	}
}
