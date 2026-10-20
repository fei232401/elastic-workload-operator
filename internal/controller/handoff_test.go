package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

type fakeCheckpoints struct {
	complete  bool
	err       error
	step      int64
	endpoints []string
	ids       []int64
}

func (f *fakeCheckpoints) Request(_ context.Context, endpoint string, id int64) (CheckpointStatus, error) {
	f.endpoints = append(f.endpoints, endpoint)
	f.ids = append(f.ids, id)
	if f.err != nil {
		return CheckpointStatus{}, f.err
	}
	if f.complete {
		return CheckpointStatus{CheckpointID: id, Step: f.step}, nil
	}
	return CheckpointStatus{CheckpointID: id - 1}, nil
}

func newHandoffEnv(t *testing.T, ttft int32) (*ElasticWorkloadReconciler, *fakeCheckpoints, *events.FakeRecorder) {
	t.Helper()
	r, _ := newTestEnv(t, ttft, nil)
	ew := fetchTestWorkload(t, r)
	ew.Spec.Training.Checkpoint = &aiinfrav1alpha1.CheckpointSpec{TimeoutSeconds: 30, ClaimName: "ckpt"}
	if err := r.Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	checkpoints := &fakeCheckpoints{step: 1234}
	recorder := events.NewFakeRecorder(50)
	r.Checkpoints, r.Recorder = checkpoints, recorder
	return r, checkpoints, recorder
}

func markAvailable(t *testing.T, r *ElasticWorkloadReconciler, training, inference int32) {
	t.Helper()
	ctx := context.Background()
	set := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: "demo-training", Namespace: "default"}, set); err != nil {
		t.Fatal(err)
	}
	set.Status = appsv1.StatefulSetStatus{ObservedGeneration: set.Generation, Replicas: training, UpdatedReplicas: training, ReadyReplicas: training, AvailableReplicas: training}
	if err := r.Status().Update(ctx, set); err != nil {
		t.Fatal(err)
	}
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Name: "demo-inference", Namespace: "default"}, deployment); err != nil {
		t.Fatal(err)
	}
	deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: deployment.Generation, Replicas: inference, UpdatedReplicas: inference, ReadyReplicas: inference, AvailableReplicas: inference}
	if err := r.Status().Update(ctx, deployment); err != nil {
		t.Fatal(err)
	}
}

func createPods(t *testing.T, r *ElasticWorkloadReconciler, workload string, count int) []*corev1.Pod {
	t.Helper()
	pods := make([]*corev1.Pod, 0, count)
	for index := range count {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("demo-%s-%d", workload, index), Namespace: "default", Labels: workloadLabels("demo", workload),
		}}
		if err := r.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
		pods = append(pods, pod)
	}
	return pods
}

func handoffPhase(t *testing.T, r *ElasticWorkloadReconciler) string {
	t.Helper()
	if h := fetchTestWorkload(t, r).Status.Handoff; h != nil {
		return h.Phase
	}
	return ""
}

func drainEvents(recorder *events.FakeRecorder) string {
	var seen []string
	for {
		select {
		case event := <-recorder.Events:
			seen = append(seen, event)
		default:
			return strings.Join(seen, "\n")
		}
	}
}

func TestHandoff_CheckpointsBeforeReleasingTrainingThenGrowsInference(t *testing.T) {
	r, checkpoints, recorder := newHandoffEnv(t, 412)
	pods := createPods(t, r, WorkloadTraining, 4)

	reconcileTestWorkload(t, r)
	if phase := handoffPhase(t, r); phase != handoffCheckpointRequested {
		t.Fatalf("want CheckpointRequested, got %q", phase)
	}
	if replicasOf(t, r, WorkloadTraining) != 4 || replicasOf(t, r, WorkloadInference) != 2 {
		t.Fatal("replicas changed before the checkpoint was confirmed")
	}
	if checkpoints.endpoints[0] != "http://demo-training-0.demo-training.default.svc:8080" || checkpoints.ids[0] <= 0 {
		t.Fatalf("unexpected checkpoint request %v %v", checkpoints.endpoints, checkpoints.ids)
	}
	if got := fetchTestWorkload(t, r).Status.DesiredReplicas; got.Training != 3 || got.Inference != 3 {
		t.Fatalf("committed target must be recorded up front, got %+v", got)
	}

	checkpoints.complete = true
	reconcileTestWorkload(t, r)
	h := fetchTestWorkload(t, r).Status.Handoff
	if h == nil || h.Phase != handoffReleasing || h.CheckpointStep != 1234 || h.CheckpointedAt == nil {
		t.Fatalf("checkpoint confirmation not recorded: %+v", h)
	}
	if replicasOf(t, r, WorkloadTraining) != 3 || replicasOf(t, r, WorkloadInference) != 2 {
		t.Fatal("releasing phase must shrink training only")
	}

	reconcileTestWorkload(t, r)
	if phase := handoffPhase(t, r); phase != handoffReleasing {
		t.Fatalf("four training pods still exist; want Releasing, got %q", phase)
	}
	if err := r.Delete(context.Background(), pods[3]); err != nil {
		t.Fatal(err)
	}
	reconcileTestWorkload(t, r)
	if phase := handoffPhase(t, r); phase != handoffAcquiring || replicasOf(t, r, WorkloadInference) != 3 {
		t.Fatalf("inference must grow only after training pods are gone; phase=%q", phase)
	}

	markAvailable(t, r, 3, 3)
	reconcileTestWorkload(t, r)
	got := fetchTestWorkload(t, r)
	if got.Status.Handoff != nil || got.Status.Phase != "Running" {
		t.Fatalf("handoff not completed: %+v", got.Status)
	}
	emitted := drainEvents(recorder)
	for _, reason := range []string{"DecisionCommitted", "CheckpointCompleted", "HandoffCompleted"} {
		if !strings.Contains(emitted, reason) {
			t.Fatalf("missing %s event in:\n%s", reason, emitted)
		}
	}
}

func TestHandoff_TimeoutReleasesWithoutCheckpoint(t *testing.T) {
	r, checkpoints, recorder := newHandoffEnv(t, 412)
	checkpoints.err = errors.New("connection refused")
	reconcileTestWorkload(t, r)
	ew := fetchTestWorkload(t, r)
	ew.Status.Handoff.RequestedAt = metav1.NewTime(time.Now().Add(-time.Minute))
	if err := r.Status().Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	reconcileTestWorkload(t, r)
	h := fetchTestWorkload(t, r).Status.Handoff
	if h == nil || h.Phase != handoffReleasing || !h.CheckpointTimedOut || h.CheckpointedAt != nil {
		t.Fatalf("timeout must release without a checkpoint: %+v", h)
	}
	if replicasOf(t, r, WorkloadTraining) != 3 {
		t.Fatal("training not released after timeout")
	}
	if emitted := drainEvents(recorder); !strings.Contains(emitted, "CheckpointTimeout") || !strings.Contains(emitted, "connection refused") {
		t.Fatalf("timeout event missing cause:\n%s", emitted)
	}
}

func TestHandoff_RecoveryReleasesInferenceBeforeGrowingTraining(t *testing.T) {
	r, checkpoints, _ := newHandoffEnv(t, 100)
	checkpoints.complete = true
	reconcileTestWorkload(t, r)
	reconcileTestWorkload(t, r)
	if phase := handoffPhase(t, r); phase != handoffAcquiring {
		t.Fatalf("no inference pods remain, want Acquiring, got %q", phase)
	}
	if replicasOf(t, r, WorkloadInference) != 1 || replicasOf(t, r, WorkloadTraining) != 5 {
		t.Fatal("recovery must shrink inference, then grow training")
	}
}

func TestHandoff_SuspendsNewDecisions(t *testing.T) {
	r, _, _ := newHandoffEnv(t, 412)
	metrics := &countingMetrics{value: 412}
	r.Metrics = metrics
	reconcileTestWorkload(t, r)
	for range 3 {
		reconcileTestWorkload(t, r)
	}
	if metrics.calls != 1 || len(fetchTestWorkload(t, r).Status.ScalingHistory) != 1 {
		t.Fatalf("decisions must pause during a handoff: calls=%d", metrics.calls)
	}
}

func TestTraining_StatefulSetWiring(t *testing.T) {
	r, _, _ := newHandoffEnv(t, 210)
	reconcileTestWorkload(t, r)
	set := &appsv1.StatefulSet{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-training", Namespace: "default"}, set); err != nil {
		t.Fatal(err)
	}
	pod := set.Spec.Template.Spec
	env := map[string]string{}
	for _, variable := range pod.Containers[0].Env {
		env[variable.Name] = variable.Value
	}
	if set.Spec.ServiceName != "demo-training" || set.Spec.PodManagementPolicy != appsv1.ParallelPodManagement ||
		env["RDZV_ENDPOINT"] != "demo-training-0.demo-training:29400" || env["MIN_NODES"] != "2" || env["MAX_NODES"] != "8" ||
		env["CHECKPOINT_DIR"] != "/ckpt" || pod.Volumes[0].PersistentVolumeClaim.ClaimName != "ckpt" ||
		pod.Containers[0].ReadinessProbe.HTTPGet.Path != "/healthz" {
		t.Fatalf("unexpected training wiring: %+v env=%v", set.Spec, env)
	}
	service := &corev1.Service{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-training", Namespace: "default"}, service); err != nil {
		t.Fatal(err)
	}
	if service.Spec.ClusterIP != corev1.ClusterIPNone || !service.Spec.PublishNotReadyAddresses {
		t.Fatalf("rendezvous needs a headless service publishing unready pods: %+v", service.Spec)
	}
}

func TestHandoff_IDsIncreaseAndIgnoreFinishedPods(t *testing.T) {
	r, checkpoints, _ := newHandoffEnv(t, 412)
	ew := fetchTestWorkload(t, r)
	future := time.Now().Add(time.Hour).UnixMilli()
	ew.Status.LastCheckpointID = future
	if err := r.Status().Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	pods := createPods(t, r, WorkloadTraining, 4)
	pods[3].Status.Phase = corev1.PodFailed
	if err := r.Status().Update(context.Background(), pods[3]); err != nil {
		t.Fatal(err)
	}
	reconcileTestWorkload(t, r)
	if got := fetchTestWorkload(t, r).Status.Handoff.CheckpointID; got != future+1 {
		t.Fatalf("checkpoint id must keep increasing across clock skew: got %d want %d", got, future+1)
	}
	checkpoints.complete = true
	reconcileTestWorkload(t, r)
	reconcileTestWorkload(t, r)
	if phase := handoffPhase(t, r); phase != handoffAcquiring {
		t.Fatalf("a failed pod must not block release, phase=%q", phase)
	}
}

func TestTraining_ReplacesLegacyDeploymentAndRequiresClaim(t *testing.T) {
	r, _, _ := newHandoffEnv(t, 210)
	ew := fetchTestWorkload(t, r)
	legacy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "demo-training", Namespace: "default", Labels: workloadLabels("demo", WorkloadTraining)}}
	owner := true
	legacy.OwnerReferences = []metav1.OwnerReference{{APIVersion: "aiinfra.example.com/v1alpha1", Kind: "ElasticWorkload", Name: "demo", UID: ew.UID, Controller: &owner}}
	if err := r.Create(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}
	reconcileTestWorkload(t, r)
	if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-training", Namespace: "default"}, &appsv1.Deployment{}); err == nil {
		t.Fatal("legacy training deployment must be removed")
	}
	ew = fetchTestWorkload(t, r)
	ew.Spec.Training.Checkpoint.ClaimName = ""
	if validateAllocation(ew) == nil {
		t.Fatal("checkpoint without a shared claim must be rejected")
	}
}
