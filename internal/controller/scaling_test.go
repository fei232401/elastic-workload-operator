package controller

import (
	"testing"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

// 测试目标 = "翻译是否正确"(设计分层 L1);决策算法本身的正确性由项目一 135 tests 覆盖,不重测。

func policy() *aiinfrav1alpha1.ScalingPolicy {
	return &aiinfrav1alpha1.ScalingPolicy{
		TargetMetric:    "ttft_p95",
		SLO:             300, // 项目一:300ms
		MinHoldDuration: 24,  // 项目一:24 tick @5s = 120s
		MaxScaleStep:    1,   // 项目一:渐进 ±1
	}
}

func cur(t, i int32) *aiinfrav1alpha1.WorkloadReplicas {
	return &aiinfrav1alpha1.WorkloadReplicas{Training: t, Inference: i}
}

func TestDecide(t *testing.T) {
	const (
		tMin, tMax = int32(2), int32(8)
		iMin, iMax = int32(1), int32(4)
	)
	p := policy()
	cases := []struct {
		name string
		ttft int32
		cur  *aiinfrav1alpha1.WorkloadReplicas
		want string
		dI   int32
		dT   int32
		wT   int32 // 目标训练副本数(声明式收敛意图)
		wI   int32 // 目标推理副本数
	}{
		{"过载→让渡:推理+1 训练-1", 412, cur(4, 2), "scale_inference_up", 1, -1, 3, 3},
		{"恰好 SLO=300 属死区(规则是 > 才过载)", 300, cur(4, 2), "none", 0, 0, 4, 2},
		{"0.6×SLO=180 是下边界,恰好=180 也属死区(规则是 < 才归还)", 180, cur(4, 2), "none", 0, 0, 4, 2},
		{"健康→归还:推理-1 训练+1(179 < 180)", 179, cur(4, 2), "scale_inference_down", -1, 1, 5, 1},
		{"死区 210(0.7×SLO)→不动", 210, cur(4, 2), "none", 0, 0, 4, 2},
		{"过载但推理已到 max=4→不动", 412, cur(4, 4), "none", 0, 0, 4, 4},
		{"过载但训练已到 min=2→不动", 412, cur(2, 3), "none", 0, 0, 2, 3},
		{"健康但推理已到 min=1→不动", 180, cur(8, 1), "none", 0, 0, 8, 1},
		{"健康但训练已到 max=8→不动", 180, cur(8, 3), "none", 0, 0, 8, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decide(tc.ttft, tc.cur, p, tMin, tMax, iMin, iMax)
			if got.Action != tc.want || got.InferenceDelta != tc.dI || got.TrainingDelta != tc.dT ||
				got.DesiredTraining != tc.wT || got.DesiredInference != tc.wI {
				t.Fatalf("decide(%d, %+v) = action=%q dI=%d dT=%d desired(T=%d,I=%d), want action=%q dI=%d dT=%d desired(T=%d,I=%d)",
					tc.ttft, *tc.cur, got.Action, got.InferenceDelta, got.TrainingDelta,
					got.DesiredTraining, got.DesiredInference,
					tc.want, tc.dI, tc.dT, tc.wT, tc.wI)
			}
		})
	}
}

func TestDecide_StepClampedNearBoundary(t *testing.T) {
	// 步长=2,但推理距 max 只差 1 → 实际只让 1(钳制,不越界),目标=训练4/推理... 3→4
	p := policy()
	p.MaxScaleStep = 2
	got := decide(412, cur(4, 3), p, 2, 8, 1, 4)
	if got.Action != "scale_inference_up" || got.InferenceDelta != 1 || got.TrainingDelta != -1 ||
		got.DesiredTraining != 3 || got.DesiredInference != 4 {
		t.Fatalf("近边界钳制: got %+v, want +1/-1 目标(3,4)", got)
	}
}

func TestDecide_BothSidesBound(t *testing.T) {
	// 推理可 +2、训练可 -2,但训练只剩 1 可让(3→2) → 步长钳到 1,目标=训练2/推理3
	p := policy()
	p.MaxScaleStep = 2
	got := decide(412, cur(3, 2), p, 2, 8, 1, 4)
	if got.Action != "scale_inference_up" || got.InferenceDelta != 1 || got.TrainingDelta != -1 ||
		got.DesiredTraining != 2 || got.DesiredInference != 3 {
		t.Fatalf("双边界钳制: got %+v, want +1/-1 目标(2,3)", got)
	}
}
