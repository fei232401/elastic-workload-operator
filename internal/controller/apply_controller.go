package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

// commit 把决策"意图"原子写进 status(single source of truth,幕8 声明式目标收敛):
//   - status.desiredReplicas = 本次决策的目标分配
//   - status.scalingHistory 追加一条审计(兼作下一次 min_hold 计时起点)
//   - 发一条 Scaled Event
//
// 单对象 Status().Update 天然原子 —— 这就是修复旧"两段式写非原子丢卡(6→5)"的关键:
// 意图一旦提交,apply 只会朝它收敛;部分失败(写成训练、冲突推理)重试时,
// 训练已到位是 no-op、推理继续朝同一目标收敛,总数不丢、不叠加漂移。
func (r *ElasticWorkloadReconciler) commit(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload, dec ScalingDecision) error {
	logger := logf.FromContext(ctx)
	ew.Status.DesiredReplicas = aiinfrav1alpha1.WorkloadReplicas{
		Training:  dec.DesiredTraining,
		Inference: dec.DesiredInference,
	}
	// 审计轨迹(面试:status 兼作控制器状态与审计,重启安全)
	ew.Status.ScalingHistory = append(ew.Status.ScalingHistory, aiinfrav1alpha1.ScalingEvent{
		Time:   metav1.Now(),
		Action: fmt.Sprintf("inference%+d/training%+d", dec.InferenceDelta, dec.TrainingDelta),
		Reason: decisionReason(dec),
		Metric: fmt.Sprintf("ttft_p95=%dms", dec.TTFTMS),
	})
	if err := r.Status().Update(ctx, ew); err != nil {
		return err
	}
	logger.Info("committed", "desiredTraining", dec.DesiredTraining, "desiredInference", dec.DesiredInference, "reason", decisionReason(dec))
	// Event(幕6b):describe 可见的操作日志
	r.event(ew, corev1.EventTypeNormal, "Scaled",
		fmt.Sprintf("inference=%d/training=%d (%s, ttft_p95=%dms)",
			dec.DesiredInference, dec.DesiredTraining, decisionReason(dec), dec.TTFTMS))
	return nil
}

// apply 收敛两个子 Deployment 的 spec.replicas 到 status.desiredReplicas(已提交的意图)。
//   - 幂等:cur == target 不写(少一次 API 写,不覆盖外部改动)
//   - 乐观并发:Update 带 resourceVersion,冲突返回 error → controller-runtime 退避重试
//   - 收敛语义:朝"目标值"而非"delta"收敛 —— 部分失败重试时训练已到位是 no-op,
//     推理继续朝同一目标补上,总数不丢(旧版 delta 叠加会把 6 拖成 5,本版根治)
func (r *ElasticWorkloadReconciler) apply(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) error {
	logger := logf.FromContext(ctx)
	desired := ew.Status.DesiredReplicas

	for _, u := range []struct {
		workload string
		target   int32
	}{
		{WorkloadTraining, desired.Training},
		{WorkloadInference, desired.Inference},
	} {
		var dep appsv1.Deployment
		if err := r.Get(ctx, types.NamespacedName{Name: ew.Name + "-" + u.workload, Namespace: ew.Namespace}, &dep); err != nil {
			return err
		}
		cur := int32(0)
		if dep.Spec.Replicas != nil {
			cur = *dep.Spec.Replicas
		}
		if cur == u.target {
			continue // 已收敛,不写
		}
		target := u.target
		dep.Spec.Replicas = &target
		if err := r.Update(ctx, &dep); err != nil {
			return err
		}
		logger.Info("applied", "workload", u.workload, "replicas", target)
	}
	return nil
}

// decisionReason 把动作翻译成语义(项目一术语:过载/健康归还)
func decisionReason(dec ScalingDecision) string {
	switch dec.Action {
	case "scale_inference_up":
		return "overload"
	case "scale_inference_down":
		return "recovery"
	}
	return "none"
}
