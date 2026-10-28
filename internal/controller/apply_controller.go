package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

func (r *ElasticWorkloadReconciler) commit(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload, dec ScalingDecision) error {
	logger := logf.FromContext(ctx)
	from := ew.Status.DesiredReplicas
	ew.Status.DesiredReplicas = aiinfrav1alpha1.WorkloadReplicas{
		Training:  dec.DesiredTraining,
		Inference: dec.DesiredInference,
	}
	if ew.Spec.Training.Checkpoint != nil && from.Training != dec.DesiredTraining {
		now := metav1.Now()
		ew.Status.LastCheckpointID = max(now.UnixMilli(), ew.Status.LastCheckpointID+1)
		ew.Status.Handoff = &aiinfrav1alpha1.Handoff{
			Phase: handoffCheckpointRequested, From: from, To: ew.Status.DesiredReplicas,
			CheckpointID: ew.Status.LastCheckpointID, RequestedAt: now,
			PhaseStartedAt: &now, SpecSnapshot: ew.Spec.DeepCopy(),
		}
		if from.Training == 0 {
			ew.Status.Handoff.Phase = handoffReleasing
		}
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
	ew.Status.Phase = phaseProgressing
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
	targets := effectiveTargets(ew)
	for _, u := range []struct {
		workload string
		target   int32
	}{
		{WorkloadTraining, targets.Training},
		{WorkloadInference, targets.Inference},
	} {
		child, err := r.getChild(ctx, ew, u.workload)
		if err != nil {
			return err
		}
		if current := childReplicas(child); current != nil && *current == u.target {
			continue
		}
		setChildReplicas(child, u.target)
		if err := r.Update(ctx, child); err != nil {
			return err
		}
		logger.Info("applied", "workload", u.workload, "replicas", u.target)
	}
	return nil
}

func decisionReason(dec ScalingDecision) string {
	switch dec.Action {
	case actionScaleInferenceUp:
		return reasonOverload
	case actionScaleInferenceDown:
		return reasonRecovery
	}
	return actionNone
}
