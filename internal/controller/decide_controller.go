package controller

import (
	"context"
	"fmt"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

// tickSeconds 决策周期 = 5s(项目一 tick;字段注释 "@5s = 120s" 即 minHoldDuration × tickSeconds)
const tickSeconds = 5 * time.Second

// MetricsReader 读路由器(数据面 = KV Router Python 服务)的 ttft_p95。
// 真实现(幕7 L2):HTTP GET Router 指标接口;单测注入 fake。
type MetricsReader interface {
	TTFTP95MS(ctx context.Context) (int32, error)
}

// decideStep 是幕4:读当前分配 + 读 ttft_p95 + min_hold 闸门 → decide()。
// 返回决策(幕4 只记日志不 apply;幕5 才落 Deployment replicas)。
func (r *ElasticWorkloadReconciler) decideStep(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) (ScalingDecision, error) {
	logger := logf.FromContext(ctx)
	if r.Metrics == nil {
		return ScalingDecision{}, fmt.Errorf("MetricsReader not configured")
	}

	alloc := ew.Status.DesiredReplicas

	ttft, err := r.Metrics.TTFTP95MS(ctx)
	if err != nil {
		return ScalingDecision{}, err
	}
	if ttft < 0 {
		return ScalingDecision{}, fmt.Errorf("ttft_p95_ms must be non-negative")
	}
	logger.Info("observe metric", "ttft_p95_ms", ttft, "allocation", alloc)

	// min_hold 闸门:上次扩缩后 minHoldDuration×5s 内不再动(项目一 Rule4)
	// 上次扩缩时间存 status.scalingHistory 末位 → 无内存态,重启安全
	if last := lastScaleTime(ew); last != nil {
		hold := time.Duration(ew.Spec.ScalingPolicy.MinHoldDuration) * tickSeconds
		if since := time.Since(*last); since < hold {
			logger.Info("min_hold: skip", "since", since.Round(time.Second), "hold", hold)
			return ScalingDecision{Action: "none"}, nil
		}
	}

	dec := decide(ttft, &alloc, &ew.Spec.ScalingPolicy,
		ew.Spec.Training.MinReplicas, ew.Spec.Training.MaxReplicas,
		ew.Spec.Inference.MinReplicas, ew.Spec.Inference.MaxReplicas)
	dec.TTFTMS = ttft
	logger.Info("decision", "action", dec.Action, "inferenceDelta", dec.InferenceDelta, "trainingDelta", dec.TrainingDelta)
	return dec, nil
}

// lastScaleTime 取最近一次扩缩时间(存 status.scalingHistory 末位)
func lastScaleTime(ew *aiinfrav1alpha1.ElasticWorkload) *time.Time {
	if n := len(ew.Status.ScalingHistory); n > 0 {
		t := ew.Status.ScalingHistory[n-1].Time.Time
		return &t
	}
	return nil
}
