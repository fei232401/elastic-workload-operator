package controller

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

// TestGPULimits:gpuPerWorker/GPUPerInstance=0 → 不请求 GPU(无 GPU 集群/数据面 mock 场景);
// >0 → 请求 nvidia.com/gpu。L2 靠 0 让 busybox 真正 Ready。
func TestGPULimits_Zero_NoGPU(t *testing.T) {
	if got := gpuLimits(0); got != nil {
		t.Fatalf("want nil limits for gpus=0, got %v", got)
	}
}

func TestGPULimits_NonZero_RequestsGPU(t *testing.T) {
	got := gpuLimits(2)
	want := resource.MustParse("2")
	if len(got) != 1 {
		t.Fatalf("want 1 resource, got %v", got)
	}
	gv, wv := got["nvidia.com/gpu"], want
	if gv.Cmp(wv) != 0 {
		t.Fatalf("want nvidia.com/gpu=%s, got %s", wv.String(), gv.String())
	}
}

func TestGPULimits_Negative_NoGPU(t *testing.T) {
	if got := gpuLimits(-1); got != nil {
		t.Fatalf("want nil limits for negative gpus, got %v", got)
	}
}
