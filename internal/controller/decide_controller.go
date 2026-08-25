package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
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

	// 决策输入 = 当前分配(Deployment spec.replicas),不是就绪数(ready 有 provisioning 延迟,
	// 跟项目一"分配即时生效"对齐;就绪数由 observe 另写 status)
	alloc, err := r.allocation(ctx, ew)
	if err != nil {
		return ScalingDecision{}, err
	}

	ttft, err := r.Metrics.TTFTP95MS(ctx)
	if err != nil {
		return ScalingDecision{}, err
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

// allocation 读两个子 Deployment 当前 spec.replicas(我们控制的分配)
func (r *ElasticWorkloadReconciler) allocation(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) (aiinfrav1alpha1.WorkloadReplicas, error) {
	var out aiinfrav1alpha1.WorkloadReplicas
	for _, wl := range []string{WorkloadTraining, WorkloadInference} {
		var dep appsv1.Deployment
		if err := r.Get(ctx, types.NamespacedName{Name: ew.Name + "-" + wl, Namespace: ew.Namespace}, &dep); err != nil {
			return out, err
		}
		n := int32(0)
		if dep.Spec.Replicas != nil {
			n = *dep.Spec.Replicas
		}
		switch wl {
		case WorkloadTraining:
			out.Training = n
		case WorkloadInference:
			out.Inference = n
		}
	}
	return out, nil
}

// lastScaleTime 取最近一次扩缩时间(存 status.scalingHistory 末位)
func lastScaleTime(ew *aiinfrav1alpha1.ElasticWorkload) *time.Time {
	if n := len(ew.Status.ScalingHistory); n > 0 {
		t := ew.Status.ScalingHistory[n-1].Time.Time
		return &t
	}
	return nil
}
