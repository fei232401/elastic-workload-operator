package controller

import (
	"context"
	"fmt"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

const tickInterval = 5 * time.Second

type MetricsReader interface {
	TTFTP95MS(ctx context.Context, source aiinfrav1alpha1.MetricSource) (int32, error)
}

func metricSource(ew *aiinfrav1alpha1.ElasticWorkload) aiinfrav1alpha1.MetricSource {
	if ew.Spec.ScalingPolicy.Metrics == nil {
		return aiinfrav1alpha1.MetricSource{}
	}
	return *ew.Spec.ScalingPolicy.Metrics
}

func (r *ElasticWorkloadReconciler) decideStep(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) (ScalingDecision, error) {
	logger := logf.FromContext(ctx)
	if r.Metrics == nil {
		return ScalingDecision{}, fmt.Errorf("MetricsReader not configured")
	}

	alloc := ew.Status.DesiredReplicas

	ttft, err := r.Metrics.TTFTP95MS(ctx, metricSource(ew))
	if err != nil {
		return ScalingDecision{}, err
	}
	if ttft < 0 {
		return ScalingDecision{}, fmt.Errorf("ttft_p95_ms must be non-negative")
	}
	logger.Info("observe metric", "ttft_p95_ms", ttft, "allocation", alloc)

	if last := lastScaleTime(ew); last != nil {
		hold := time.Duration(ew.Spec.ScalingPolicy.MinHoldDuration) * tickInterval
		if since := time.Since(*last); since < hold {
			logger.Info("min_hold: skip", "since", since.Round(time.Second), "hold", hold)
			return ScalingDecision{Action: actionNone}, nil
		}
	}

	dec := decide(ttft, &alloc, &ew.Spec.ScalingPolicy,
		ew.Spec.Training.MinReplicas, ew.Spec.Training.MaxReplicas,
		ew.Spec.Inference.MinReplicas, ew.Spec.Inference.MaxReplicas)
	dec.TTFTMS = ttft
	logger.Info("decision", "action", dec.Action, "inferenceDelta", dec.InferenceDelta, "trainingDelta", dec.TrainingDelta)
	return dec, nil
}

func lastScaleTime(ew *aiinfrav1alpha1.ElasticWorkload) *time.Time {
	if n := len(ew.Status.ScalingHistory); n > 0 {
		t := ew.Status.ScalingHistory[n-1].Time.Time
		return &t
	}
	return nil
}
