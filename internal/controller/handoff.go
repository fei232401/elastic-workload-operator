package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

const (
	handoffCheckpointRequested = "CheckpointRequested"
	handoffReleasing           = "Releasing"
	handoffAcquiring           = "Acquiring"
	handoffPoll                = time.Second
)

type CheckpointStatus struct {
	CheckpointID int64 `json:"checkpoint_id"`
	Step         int64 `json:"step"`
}

type CheckpointClient interface {
	Request(ctx context.Context, endpoint string, id int64) (CheckpointStatus, error)
}

type httpCheckpointClient struct {
	client *http.Client
}

func NewHTTPCheckpointClient() CheckpointClient {
	return &httpCheckpointClient{client: &http.Client{Timeout: 2 * time.Second}}
}

func (c *httpCheckpointClient) Request(ctx context.Context, endpoint string, id int64) (CheckpointStatus, error) {
	var status CheckpointStatus
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/checkpoint?id=%d", endpoint, id), nil)
	if err != nil {
		return status, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return status, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusAccepted {
		return status, fmt.Errorf("trainer returned status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		return status, err
	}
	return status, json.Unmarshal(body, &status)
}

func releasingWorkload(h *aiinfrav1alpha1.Handoff) string {
	if h.To.Training < h.From.Training {
		return WorkloadTraining
	}
	return WorkloadInference
}

func effectiveTargets(ew *aiinfrav1alpha1.ElasticWorkload) aiinfrav1alpha1.WorkloadReplicas {
	h := ew.Status.Handoff
	if h == nil {
		return ew.Status.DesiredReplicas
	}
	switch h.Phase {
	case handoffCheckpointRequested:
		return h.From
	case handoffReleasing:
		targets := h.From
		if releasingWorkload(h) == WorkloadTraining {
			targets.Training = h.To.Training
		} else {
			targets.Inference = h.To.Inference
		}
		return targets
	default:
		return h.To
	}
}

func (r *ElasticWorkloadReconciler) reconcileHandoff(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) (ctrl.Result, error) {
	previous := ew.DeepCopy()
	h := ew.Status.Handoff
	switch h.Phase {
	case handoffCheckpointRequested:
		r.advanceCheckpoint(ctx, ew)
	case handoffReleasing:
		released, err := r.released(ctx, ew)
		if err != nil {
			return ctrl.Result{}, err
		}
		if released {
			now := metav1.Now()
			h.Phase, h.ReleasedAt = handoffAcquiring, &now
		}
	}
	if err := r.saveStatus(ctx, ew, previous); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.apply(ctx, ew); err != nil {
		return ctrl.Result{}, err
	}
	if h.Phase == handoffAcquiring {
		_, available, _, err := r.inspectWorkloads(ctx, ew)
		if err != nil {
			return ctrl.Result{}, err
		}
		if available {
			before := ew.DeepCopy()
			r.completeHandoff(ew)
			if err := r.saveStatus(ctx, ew, before); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	if _, err := r.reportWorkloads(ctx, ew); err != nil {
		return ctrl.Result{}, err
	}
	if ew.Status.Handoff == nil {
		return ctrl.Result{RequeueAfter: tickInterval}, nil
	}
	return ctrl.Result{RequeueAfter: handoffPoll}, nil
}

func (r *ElasticWorkloadReconciler) advanceCheckpoint(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) {
	h := ew.Status.Handoff
	_, _, timeout := checkpointSettings(ew)
	var (
		status CheckpointStatus
		err    = fmt.Errorf("checkpoint client is not configured")
	)
	if r.Checkpoints != nil {
		status, err = r.Checkpoints.Request(ctx, checkpointEndpoint(ew), h.CheckpointID)
	}
	if err == nil && status.CheckpointID >= h.CheckpointID {
		now := metav1.Now()
		h.Phase, h.CheckpointedAt, h.CheckpointStep = handoffReleasing, &now, status.Step
		r.event(ew, corev1.EventTypeNormal, "CheckpointCompleted",
			fmt.Sprintf("checkpoint %d saved at step %d after %s", h.CheckpointID, status.Step, now.Sub(h.RequestedAt.Time).Round(time.Millisecond)))
		return
	}
	if time.Since(h.RequestedAt.Time) < time.Duration(timeout)*time.Second {
		if err != nil {
			logf.FromContext(ctx).Info("checkpoint not confirmed yet", "id", h.CheckpointID, "error", err.Error())
		}
		return
	}
	h.Phase, h.CheckpointTimedOut = handoffReleasing, true
	message := fmt.Sprintf("checkpoint %d not confirmed within %ds; releasing without it", h.CheckpointID, timeout)
	if err != nil {
		message += ": " + err.Error()
	}
	if len(message) > 512 {
		message = message[:512]
	}
	r.event(ew, corev1.EventTypeWarning, "CheckpointTimeout", message)
}

func (r *ElasticWorkloadReconciler) released(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) (bool, error) {
	h := ew.Status.Handoff
	workload := releasingWorkload(h)
	target := h.To.Inference
	if workload == WorkloadTraining {
		target = h.To.Training
	}
	child, err := r.getChild(ctx, ew, workload)
	if err != nil {
		return false, err
	}
	if replicas := childReplicas(child); replicas == nil || *replicas != target {
		return false, nil
	}
	count, err := r.podCount(ctx, ew, workload)
	return count <= target, err
}

func (r *ElasticWorkloadReconciler) completeHandoff(ew *aiinfrav1alpha1.ElasticWorkload) {
	h := ew.Status.Handoff
	total := time.Since(h.RequestedAt.Time).Round(time.Millisecond)
	checkpoint := "timed out"
	if h.From.Training == 0 {
		checkpoint = "skipped (no running trainers)"
	}
	if h.CheckpointedAt != nil {
		checkpoint = h.CheckpointedAt.Sub(h.RequestedAt.Time).Round(time.Millisecond).String()
	}
	release := "n/a"
	if h.ReleasedAt != nil {
		start := h.RequestedAt.Time
		if h.CheckpointedAt != nil {
			start = h.CheckpointedAt.Time
		}
		release = h.ReleasedAt.Sub(start).Round(time.Millisecond).String()
	}
	r.event(ew, corev1.EventTypeNormal, "HandoffCompleted",
		fmt.Sprintf("training %d->%d, inference %d->%d; checkpoint %s, release %s, total %s",
			h.From.Training, h.To.Training, h.From.Inference, h.To.Inference, checkpoint, release, total))
	ew.Status.Handoff = nil
}
