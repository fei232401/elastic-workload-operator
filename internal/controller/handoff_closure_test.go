package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestHandoff_SpecChangesFreezeBeforeChildUpdates(t *testing.T) {
	for _, phase := range []string{handoffCheckpointRequested, handoffReleasing, handoffAcquiring} {
		t.Run(phase, func(t *testing.T) {
			r, checkpoints, recorder := newHandoffEnv(t, 412)
			reconcileTestWorkload(t, r)
			ew := fetchTestWorkload(t, r)
			original := ew.Spec.DeepCopy()
			ew.Status.Handoff.Phase = phase
			if err := r.Status().Update(context.Background(), ew); err != nil {
				t.Fatal(err)
			}
			ew.Spec.Inference.Image = "must-not-be-applied"
			ew.Spec.Training.MinReplicas = 3
			if err := r.Update(context.Background(), ew); err != nil {
				t.Fatal(err)
			}
			calls := len(checkpoints.ids)
			for range 2 {
				reconcileTestWorkload(t, r)
			}
			got := fetchTestWorkload(t, r)
			condition := apimeta.FindStatusCondition(got.Status.Conditions, conditionReady)
			if got.Status.Phase != phaseDegraded || condition == nil || condition.Reason != reasonHandoffSpecChanged {
				t.Fatalf("changed spec was not frozen: %+v", got.Status)
			}
			if len(checkpoints.ids) != calls || got.Status.Handoff.Phase != phase || len(got.Status.ScalingHistory) != 1 {
				t.Fatal("handoff advanced or new decision was made while spec was changed")
			}
			deployment := &appsv1.Deployment{}
			if err := r.Get(context.Background(), types.NamespacedName{Name: "demo-inference", Namespace: "default"}, deployment); err != nil {
				t.Fatal(err)
			}
			if deployment.Spec.Template.Spec.Containers[0].Image != original.Inference.Image || replicasOf(t, r, WorkloadTraining) != 4 || replicasOf(t, r, WorkloadInference) != 2 {
				t.Fatal("children changed before the spec guard")
			}
			if emitted := drainEvents(recorder); strings.Count(emitted, reasonHandoffSpecChanged) != 1 {
				t.Fatalf("expected one spec warning: %s", emitted)
			}
			got.Spec = *original
			if err := r.Update(context.Background(), got); err != nil {
				t.Fatal(err)
			}
			checkpoints.complete = true
			reconcileTestWorkload(t, r)
			condition = apimeta.FindStatusCondition(fetchTestWorkload(t, r).Status.Conditions, conditionReady)
			if condition == nil || condition.Reason == reasonHandoffSpecChanged {
				t.Fatal("restoring original spec did not unblock reconciliation")
			}
		})
	}
}

func TestHandoff_RecoveryWaitsForInferencePods(t *testing.T) {
	r, checkpoints, _ := newHandoffEnv(t, 100)
	pods := createPods(t, r, WorkloadInference, 2)
	checkpoints.complete = true
	reconcileTestWorkload(t, r)
	reconcileTestWorkload(t, r)
	if handoffPhase(t, r) != handoffReleasing || replicasOf(t, r, WorkloadTraining) != 4 || replicasOf(t, r, WorkloadInference) != 1 {
		t.Fatal("training grew while inference pods still held the resources")
	}
	if err := r.Delete(context.Background(), pods[1]); err != nil {
		t.Fatal(err)
	}
	reconcileTestWorkload(t, r)
	if handoffPhase(t, r) != handoffAcquiring || replicasOf(t, r, WorkloadTraining) != 5 {
		t.Fatal("training did not grow after inference released the resources")
	}
}

func TestHandoff_PhaseTimeoutSurvivesRestartAndRecovers(t *testing.T) {
	for _, phase := range []string{handoffReleasing, handoffAcquiring} {
		t.Run(phase, func(t *testing.T) {
			r, checkpoints, recorder := newHandoffEnv(t, 412)
			pods := createPods(t, r, WorkloadTraining, 4)
			checkpoints.complete = true
			reconcileTestWorkload(t, r)
			if phase == handoffAcquiring {
				if err := r.Delete(context.Background(), pods[3]); err != nil {
					t.Fatal(err)
				}
				reconcileTestWorkload(t, r)
			}
			ew := fetchTestWorkload(t, r)
			if ew.Status.Handoff.Phase != phase {
				t.Fatalf("expected %s, got %+v", phase, ew.Status.Handoff)
			}
			started := metav1.NewTime(time.Now().Add(-301 * time.Second))
			ew.Status.Handoff.PhaseStartedAt = &started
			if err := r.Status().Update(context.Background(), ew); err != nil {
				t.Fatal(err)
			}
			restarted := &ElasticWorkloadReconciler{Client: r.Client, Scheme: r.Scheme, Metrics: r.Metrics, Checkpoints: checkpoints, Recorder: recorder}
			for range 2 {
				reconcileTestWorkload(t, restarted)
			}
			got := fetchTestWorkload(t, restarted)
			if got.Status.Phase != phaseDegraded || !apimeta.IsStatusConditionTrue(got.Status.Conditions, conditionHandoffStalled) {
				t.Fatalf("timeout not reported: %+v", got.Status)
			}
			if got.Status.Handoff.Phase != phase || got.Status.DesiredReplicas != ew.Status.DesiredReplicas {
				t.Fatal("timeout must not abort or roll back the handoff")
			}
			if emitted := drainEvents(recorder); strings.Count(emitted, reasonHandoffPhaseTimeout) != 1 {
				t.Fatalf("expected one timeout warning: %s", emitted)
			}
			if phase == handoffReleasing {
				if err := r.Delete(context.Background(), pods[3]); err != nil {
					t.Fatal(err)
				}
				reconcileTestWorkload(t, restarted)
				got = fetchTestWorkload(t, restarted)
				if got.Status.Handoff.Phase != handoffAcquiring || apimeta.IsStatusConditionTrue(got.Status.Conditions, conditionHandoffStalled) {
					t.Fatal("new phase must have a fresh deadline")
				}
			}
			markAvailable(t, restarted, 3, 3)
			reconcileTestWorkload(t, restarted)
			got = fetchTestWorkload(t, restarted)
			if got.Status.Handoff != nil || got.Status.Phase != phaseRunning || apimeta.IsStatusConditionTrue(got.Status.Conditions, conditionHandoffStalled) {
				t.Fatalf("healthy workloads did not recover: %+v", got.Status)
			}
		})
	}
}

func TestHandoff_LegacySnapshotAndDeadline(t *testing.T) {
	r, checkpoints, _ := newHandoffEnv(t, 412)
	checkpoints.complete = true
	reconcileTestWorkload(t, r)
	ew := fetchTestWorkload(t, r)
	ew.Status.Handoff.SpecSnapshot = nil
	ew.Status.Handoff.PhaseStartedAt = nil
	old := metav1.NewTime(time.Now().Add(-time.Hour))
	ew.Status.Handoff.CheckpointedAt = &old
	if err := r.Status().Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	createPods(t, r, WorkloadTraining, 4)
	reconcileTestWorkload(t, r)
	got := fetchTestWorkload(t, r)
	if got.Status.Handoff.SpecSnapshot == nil || !equality.Semantic.DeepEqual(got.Spec, *got.Status.Handoff.SpecSnapshot) {
		t.Fatal("legacy handoff did not persist a spec baseline")
	}
	if !apimeta.IsStatusConditionTrue(got.Status.Conditions, conditionHandoffStalled) {
		t.Fatal("legacy phase timestamp was ignored")
	}
}

func TestHandoff_SnapshotSaveFailureStopsBeforeApply(t *testing.T) {
	r, checkpoints, _ := newHandoffEnv(t, 412)
	reconcileTestWorkload(t, r)
	ew := fetchTestWorkload(t, r)
	ew.Status.Handoff.SpecSnapshot = nil
	if err := r.Status().Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	checkpoints.complete = true
	failed := false
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, underlying client.Client, subresource string, object client.Object, options ...client.SubResourceUpdateOption) error {
			if subresource == "status" && !failed {
				failed = true
				return errors.New("snapshot write unavailable")
			}
			return underlying.SubResource(subresource).Update(ctx, object, options...)
		},
	})
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}})
	if err == nil || !failed || replicasOf(t, r, WorkloadTraining) != 4 {
		t.Fatal("snapshot write failure must stop before checkpoint and child writes")
	}
	if got := fetchTestWorkload(t, r); got.Status.Handoff.Phase != handoffCheckpointRequested || got.Status.Handoff.SpecSnapshot != nil {
		t.Fatal("failed snapshot write changed persisted handoff")
	}
	reconcileTestWorkload(t, r)
	if handoffPhase(t, r) != handoffReleasing || replicasOf(t, r, WorkloadTraining) != 3 {
		t.Fatal("retry did not save snapshot and resume")
	}
}

func TestHandoff_DeadlineConfigurationAndSnapshotCopy(t *testing.T) {
	r, _, _ := newHandoffEnv(t, 412)
	reconcileTestWorkload(t, r)
	ew := fetchTestWorkload(t, r)
	copy := ew.DeepCopy()
	copy.Status.Handoff.SpecSnapshot.Training.Command = []string{"changed"}
	if equality.Semantic.DeepEqual(ew.Status.Handoff.SpecSnapshot, copy.Status.Handoff.SpecSnapshot) {
		t.Fatal("snapshot was not deeply copied")
	}
	old := metav1.NewTime(time.Now().Add(-2 * time.Second))
	ew.Status.Handoff.PhaseStartedAt = &old
	ew.Spec.ScalingPolicy.HandoffTimeoutSeconds = 1
	if handoffTimedOut(ew) {
		t.Fatal("checkpoint must use its own timeout")
	}
	for _, phase := range []string{handoffReleasing, handoffAcquiring} {
		ew.Status.Handoff.Phase = phase
		if !handoffTimedOut(ew) {
			t.Fatal("custom timeout was ignored")
		}
		ew.Spec.ScalingPolicy.HandoffTimeoutSeconds = 0
		if handoffTimedOut(ew) {
			t.Fatal("default timeout must be 300 seconds")
		}
		ew.Spec.ScalingPolicy.HandoffTimeoutSeconds = 1
	}
	ew.Status.Handoff = nil
	if handoffTimedOut(ew) {
		t.Fatal("completed handoff cannot time out")
	}
}
