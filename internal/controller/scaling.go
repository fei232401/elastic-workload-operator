package controller

import (
	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

type ScalingDecision struct {
	Action           string
	DesiredTraining  int32
	DesiredInference int32
	InferenceDelta   int32
	TrainingDelta    int32
	TTFTMS           int32
}

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

	if int64(ttftP95MS)*10 < int64(pol.SLO)*6 {
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

	return noop(cur)
}

func noop(cur *aiinfrav1alpha1.WorkloadReplicas) ScalingDecision {
	return ScalingDecision{
		Action:           "none",
		DesiredTraining:  cur.Training,
		DesiredInference: cur.Inference,
	}
}

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
