package controller

import (
	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

// ScalingDecision 是决策引擎输出(项目一 ResourceDecision 的 K8s 翻译版)。
// 幕8 声明式目标收敛:决策输出"目标分配"(DesiredTraining/DesiredInference),
// 而非增量——意图提交后 apply 朝目标收敛,幂等,部分失败重试不叠加漂移。
// Delta 字段保留供审计轨迹(事件里的 "+1/-1")。
type ScalingDecision struct {
	Action           string // "none" | "scale_inference_up" | "scale_inference_down"
	DesiredTraining  int32  // 目标训练副本数(声明式收敛的意图)
	DesiredInference int32  // 目标推理副本数
	InferenceDelta   int32  // 审计用:本次让渡/归还量
	TrainingDelta    int32
	TTFTMS           int32 // 本次决策读到的 ttft_p95(commit 记审计轨迹用)
}

// decide 把 项目一 Rule1/Rule3 + 阈值迟滞(0.6/1.0)翻译成纯函数(无 I/O,可单测)。
// 规则(项目一 135 tests 实证,面试必讲):
//   - TTFT P95 > SLO       → 过载:推理+step、训练-step(让渡),边界未抵才动作
//   - TTFT P95 < 0.6×SLO   → 健康:推理-step、训练+step(归还),边界未抵才动作
//   - 死区 [0.6,1.0]×SLO   → 不动(阈值迟滞,防"让一下又还回来"抖动)
//
// 守恒:训练+推理总量不变(同一决策动作两边同时动);边界=各自 min/max,
// 步长在两侧可行动范围内钳制(到一侧边界则钳小,两侧都到边界则 0=不动)。
// 返回值携带目标分配:DesiredTraining/DesiredInference 已在 min/max 内(由 clampStep 保证)。
func decide(ttftP95MS int32, cur *aiinfrav1alpha1.WorkloadReplicas, pol *aiinfrav1alpha1.ScalingPolicy,
	trainingMin, trainingMax, inferenceMin, inferenceMax int32) ScalingDecision {

	if ttftP95MS > pol.SLO {
		step := clampStep(cur.Inference, inferenceMax, pol.MaxScaleStep, cur.Training, trainingMin)
		if step <= 0 {
			return noop(cur)
		}
		return ScalingDecision{
			Action:           "scale_inference_up",
			DesiredInference: cur.Inference + step,
			DesiredTraining:  cur.Training - step,
			InferenceDelta:   step,
			TrainingDelta:    -step,
		}
	}

	if ttftP95MS < pol.SLO*6/10 {
		step := clampStep(cur.Training, trainingMax, pol.MaxScaleStep, cur.Inference, inferenceMin)
		if step <= 0 {
			return noop(cur)
		}
		return ScalingDecision{
			Action:           "scale_inference_down",
			DesiredInference: cur.Inference - step,
			DesiredTraining:  cur.Training + step,
			InferenceDelta:   -step,
			TrainingDelta:    step,
		}
	}

	// 死区:不动(目标 = 当前)
	return noop(cur)
}

// noop 返回"不动"决策,目标分配 = 当前(语义完整:任何决策都携带完整目标)。
func noop(cur *aiinfrav1alpha1.WorkloadReplicas) ScalingDecision {
	return ScalingDecision{
		Action:           "none",
		DesiredTraining:  cur.Training,
		DesiredInference: cur.Inference,
	}
}

// clampStep 求两侧都可行动的最大步长。
// 上行动作:A 向 maxA 升,B 向 minB 降 → 步长受 (maxA-curA) 与 (curB-minB) 约束;
// 下行动作同理(A 向 maxA 升,B 向 minB 降)。任一为 0 → 不可动。
func clampStep(curA, maxA, maxStep, curB, minB int32) int32 {
	step := maxStep
	if d := maxA - curA; d < step {
		step = d
	}
	if d := curB - minB; d < step {
		step = d
	}
	return step
}
