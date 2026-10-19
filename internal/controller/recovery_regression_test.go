package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

type countingMetrics struct {
	calls int
	value int32
	err   error
}

func (m *countingMetrics) TTFTP95MS(context.Context) (int32, error) {
	m.calls++
	return m.value, m.err
}

func fetchTestWorkload(t *testing.T, r *ElasticWorkloadReconciler) *aiinfrav1alpha1.ElasticWorkload {
	t.Helper()
	ew := &aiinfrav1alpha1.ElasticWorkload{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "default"}, ew); err != nil {
		t.Fatal(err)
	}
	return ew
}

func reconcileTestWorkload(t *testing.T, r *ElasticWorkloadReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRecovery_CompletesOldTargetBeforeReadingUnavailableMetrics(t *testing.T) {
	r, _ := newTestEnv(t, 600, nil)
	ew := fetchTestWorkload(t, r)
	ew.Status.Phase = "Running"
	ew.Status.DesiredReplicas = aiinfrav1alpha1.WorkloadReplicas{Training: 3, Inference: 3}
	ew.Status.ScalingHistory = []aiinfrav1alpha1.ScalingEvent{{
		Time: metav1.NewTime(time.Now().Add(-time.Hour)), Action: "inference+1/training-1", Reason: "overload",
	}}
	if err := r.Status().Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	dep := &appsv1.Deployment{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-training", Namespace: "default"}, dep); err != nil {
		t.Fatal(err)
	}
	dep.Spec.Replicas = int32Ptr(3)
	if err := r.Update(context.Background(), dep); err != nil {
		t.Fatal(err)
	}
	metrics := &countingMetrics{err: errors.New("metrics offline")}
	r.Metrics = metrics
	reconcileTestWorkload(t, r)
	for _, name := range []string{"demo-training", "demo-inference"} {
		if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, dep); err != nil {
			t.Fatal(err)
		}
		if *dep.Spec.Replicas != 3 {
			t.Fatalf("%s did not converge to committed target: %d", name, *dep.Spec.Replicas)
		}
	}
	got := fetchTestWorkload(t, r)
	if metrics.calls != 0 || len(got.Status.ScalingHistory) != 1 || got.Status.Phase != "Progressing" {
		t.Fatalf("new decision attempted before workloads converged: calls=%d status=%+v", metrics.calls, got.Status)
	}
}

func TestRecovery_FinalizerWithoutInitializedStatus(t *testing.T) {
	r, _ := newTestEnv(t, 210, nil)
	ew := fetchTestWorkload(t, r)
	ew.Finalizers = []string{FinalizerName}
	if err := r.Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	ew.Status = aiinfrav1alpha1.ElasticWorkloadStatus{}
	if err := r.Status().Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	reconcileTestWorkload(t, r)
	got := fetchTestWorkload(t, r)
	if got.Status.DesiredReplicas.Training != 4 || got.Status.DesiredReplicas.Inference != 2 ||
		!apimeta.IsStatusConditionTrue(got.Status.Conditions, conditionInitialized) {
		t.Fatalf("initialization did not recover: %+v", got.Status)
	}
}

func TestRecovery_MetricsFailureRetainsAvailableAllocation(t *testing.T) {
	r, _ := newTestEnv(t, 210, nil)
	metrics := &countingMetrics{err: errors.New("metrics offline")}
	r.Metrics = metrics
	reconcileTestWorkload(t, r)
	reconcileTestWorkload(t, r)
	got := fetchTestWorkload(t, r)
	condition := apimeta.FindStatusCondition(got.Status.Conditions, conditionMetricsReady)
	if metrics.calls != 2 || condition == nil || condition.Status != metav1.ConditionFalse ||
		got.Status.DesiredReplicas.Training != 4 || got.Status.DesiredReplicas.Inference != 2 ||
		len(got.Status.ScalingHistory) != 0 {
		t.Fatalf("unavailable metric changed allocation or condition: calls=%d status=%+v", metrics.calls, got.Status)
	}
}

func TestRecovery_InvalidLegacyTargetIsNotResetOnRetry(t *testing.T) {
	r, _ := newTestEnv(t, 210, nil)
	ew := fetchTestWorkload(t, r)
	ew.Status.Phase = "Running"
	ew.Status.DesiredReplicas = aiinfrav1alpha1.WorkloadReplicas{Training: 1, Inference: 3}
	if err := r.Status().Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
		})
		if err == nil {
			t.Fatal("invalid legacy allocation should require explicit correction")
		}
	}
	got := fetchTestWorkload(t, r)
	if got.Status.DesiredReplicas.Training != 1 || got.Status.DesiredReplicas.Inference != 3 {
		t.Fatalf("invalid saved allocation was silently reset: %+v", got.Status.DesiredReplicas)
	}
}

func TestRecovery_HistoryIsBounded(t *testing.T) {
	r, _ := newTestEnv(t, 210, nil)
	ew := fetchTestWorkload(t, r)
	for i := range 100 {
		ew.Status.ScalingHistory = append(ew.Status.ScalingHistory, aiinfrav1alpha1.ScalingEvent{
			Time: metav1.NewTime(time.Now().Add(time.Duration(i-100) * time.Minute)), Action: "none",
		})
	}
	if err := r.commit(context.Background(), ew, ScalingDecision{
		Action: "scale_inference_up", DesiredTraining: 3, DesiredInference: 3, InferenceDelta: 1, TrainingDelta: -1,
	}); err != nil {
		t.Fatal(err)
	}
	got := fetchTestWorkload(t, r)
	if len(got.Status.ScalingHistory) != scalingHistoryLimit ||
		!strings.Contains(got.Status.ScalingHistory[scalingHistoryLimit-1].Action, "inference+1") {
		t.Fatalf("history not bounded or newest decision lost: %+v", got.Status.ScalingHistory)
	}
}

func TestRecovery_ActualPartialUpdateConflictIsRetried(t *testing.T) {
	r, _ := newTestEnv(t, 600, nil)
	metrics := &countingMetrics{value: 600}
	r.Metrics = metrics
	failNext := true
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, underlying client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if dep, ok := obj.(*appsv1.Deployment); ok && dep.Name == "demo-inference" && *dep.Spec.Replicas == 3 && failNext {
				failNext = false
				return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, dep.Name, errors.New("injected conflict"))
			}
			return underlying.Update(ctx, obj, opts...)
		},
	})
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}})
	if err == nil || failNext {
		t.Fatal("expected an injected update conflict")
	}
	reconcileTestWorkload(t, r)
	for _, name := range []string{"demo-training", "demo-inference"} {
		dep := &appsv1.Deployment{}
		if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, dep); err != nil {
			t.Fatal(err)
		}
		if *dep.Spec.Replicas != 3 {
			t.Fatalf("retry did not converge %s: %d", name, *dep.Spec.Replicas)
		}
	}
	if metrics.calls != 1 || len(fetchTestWorkload(t, r).Status.ScalingHistory) != 1 {
		t.Fatal("retry repeated the decision instead of restoring the saved target")
	}
}

func TestRecovery_StatusWriteFailureAfterFinalizer(t *testing.T) {
	r, _ := newTestEnv(t, 210, nil)
	ew := fetchTestWorkload(t, r)
	ew.Status = aiinfrav1alpha1.ElasticWorkloadStatus{}
	if err := r.Status().Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	failStatus := true
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, underlying client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if subresource == "status" && failStatus {
				return errors.New("injected status service failure")
			}
			return underlying.SubResource(subresource).Update(ctx, obj, opts...)
		},
	})
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}})
	if err == nil || !contains(fetchTestWorkload(t, r).Finalizers, FinalizerName) {
		t.Fatal("expected finalizer to persist despite failed status writes")
	}
	failStatus = false
	reconcileTestWorkload(t, r)
	got := fetchTestWorkload(t, r)
	if got.Status.DesiredReplicas.Training != 4 || got.Status.DesiredReplicas.Inference != 2 ||
		!apimeta.IsStatusConditionTrue(got.Status.Conditions, conditionInitialized) {
		t.Fatalf("failed initialization did not recover: %+v", got.Status)
	}
}

func TestRecovery_InvalidMetricsReasonsFreezeAllocation(t *testing.T) {
	for _, reason := range []string{"MetricStale", "InsufficientSamples", "MetricMetadataMissing", "MetricClockSkew"} {
		t.Run(reason, func(t *testing.T) {
			r, _ := newTestEnv(t, 600, nil)
			r.Metrics = &countingMetrics{err: invalidMetric(reason, "injected invalid metric")}
			reconcileTestWorkload(t, r)
			got := fetchTestWorkload(t, r)
			condition := apimeta.FindStatusCondition(got.Status.Conditions, conditionMetricsReady)
			if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != reason ||
				got.Status.DesiredReplicas.Training != 4 || got.Status.DesiredReplicas.Inference != 2 || len(got.Status.ScalingHistory) != 0 {
				t.Fatalf("invalid metric changed target or lost reason: %+v", got.Status)
			}
		})
	}
}
