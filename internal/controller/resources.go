package controller

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

func workloadResources(input aiinfrav1alpha1.WorkloadResources, gpus int32) (corev1.ResourceRequirements, error) {
	output := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
	}
	for _, entry := range []struct {
		source corev1.ResourceList
		target corev1.ResourceList
	}{{input.Requests, output.Requests}, {input.Limits, output.Limits}} {
		for name, quantity := range entry.source {
			if name != corev1.ResourceCPU && name != corev1.ResourceMemory {
				return output, fmt.Errorf("resources only accepts cpu and memory; GPU units use gpuPerWorker/gpuPerInstance")
			}
			if quantity.Sign() <= 0 {
				return output, fmt.Errorf("resource %s must be positive", name)
			}
			entry.target[name] = quantity.DeepCopy()
		}
	}
	for name, request := range output.Requests {
		limit := output.Limits[name]
		if request.Cmp(limit) > 0 {
			return output, fmt.Errorf("resource %s request exceeds its limit", name)
		}
	}
	for name, quantity := range gpuLimits(gpus) {
		output.Requests[name] = quantity
		output.Limits[name] = quantity
	}
	return output, nil
}

func readinessProbe(input *aiinfrav1alpha1.HTTPHealthCheck) (*corev1.Probe, error) {
	if input == nil {
		return nil, nil
	}
	path := input.Path
	if path == "" {
		path = "/health"
	}
	if input.Port < 1 || input.Port > 65535 || !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("readinessHTTP requires a valid port and an absolute HTTP path")
	}
	return &corev1.Probe{
		ProbeHandler:   corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromInt32(input.Port), Scheme: corev1.URISchemeHTTP}},
		TimeoutSeconds: 1, PeriodSeconds: 2, SuccessThreshold: 1, FailureThreshold: 3,
	}, nil
}
