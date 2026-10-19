package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

func TestResourceDefaultsAndGPUReservation(t *testing.T) {
	resources, err := workloadResources(aiinfrav1alpha1.WorkloadResources{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if resources.Requests.Cpu().MilliValue() != 100 || resources.Requests.Memory().Value() != 64*1024*1024 ||
		resources.Limits.Cpu().MilliValue() != 500 || resources.Limits.Memory().Value() != 256*1024*1024 ||
		resources.Requests.Name("nvidia.com/gpu", resource.DecimalSI).Value() != 2 ||
		resources.Limits.Name("nvidia.com/gpu", resource.DecimalSI).Value() != 2 {
		t.Fatalf("unexpected resource defaults: %+v", resources)
	}
}

func TestReadinessProbeContract(t *testing.T) {
	probe, err := readinessProbe(&aiinfrav1alpha1.HTTPHealthCheck{Port: 8000})
	if err != nil || probe.HTTPGet.Path != "/health" || probe.HTTPGet.Port.IntVal != 8000 || probe.PeriodSeconds != 2 {
		t.Fatalf("unexpected readiness probe: %+v %v", probe, err)
	}
	for _, input := range []*aiinfrav1alpha1.HTTPHealthCheck{{Port: 0}, {Port: 65536}, {Port: 8000, Path: "health"}} {
		if _, err := readinessProbe(input); err == nil {
			t.Fatal("invalid readiness configuration accepted")
		}
	}
}

func TestInvalidResourceConfigurations(t *testing.T) {
	for name, values := range map[string]aiinfrav1alpha1.WorkloadResources{
		"zero":         {Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}},
		"negative":     {Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("-1Gi")}},
		"over_limit":   {Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
		"gpu_override": {Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := workloadResources(values, 0); err == nil {
				t.Fatal("invalid resources accepted")
			}
		})
	}
}

func TestRejectUnsupportedResourceUnitsAndEngineSwitch(t *testing.T) {
	for _, name := range []string{"gpu", "cpu", "prefix"} {
		t.Run(name, func(t *testing.T) {
			_, ew := newTestEnv(t, 210, nil)
			switch name {
			case "gpu":
				ew.Spec.Inference.GPUPerInstance = 2
			case "cpu":
				ew.Spec.Inference.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")}
			case "prefix":
				ew.Spec.Inference.PrefixCaching = true
			}
			if validateAllocation(ew) == nil {
				t.Fatal("unsupported configuration accepted")
			}
		})
	}
}

func TestTemplateUpdatesManagedFieldsAndPreservesInjection(t *testing.T) {
	r, _ := newTestEnv(t, 210, nil)
	ew := fetchTestWorkload(t, r)
	key := types.NamespacedName{Name: "demo-inference", Namespace: "default"}
	dep := &appsv1.Deployment{}
	if err := r.Get(context.Background(), key, dep); err != nil {
		t.Fatal(err)
	}
	dep.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "INJECTED", Value: "preserved"}}
	dep.Spec.Template.Spec.Containers = append(dep.Spec.Template.Spec.Containers, corev1.Container{Name: "sidecar", Image: "observer:v1"})
	if err := r.Update(context.Background(), dep); err != nil {
		t.Fatal(err)
	}
	ew.Spec.Inference.Image = "serving:v2"
	ew.Spec.Inference.Command = []string{"/serve", "--version=2"}
	ew.Spec.Inference.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")}
	if err := r.ensureDeployment(context.Background(), ew, WorkloadInference); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), key, dep); err != nil {
		t.Fatal(err)
	}
	container := dep.Spec.Template.Spec.Containers[0]
	if container.Image != "serving:v2" || len(container.Command) != 2 || container.Resources.Limits.Memory().Value() != 512*1024*1024 ||
		len(container.Env) != 1 || len(dep.Spec.Template.Spec.Containers) != 2 || *dep.Spec.Replicas != 2 {
		t.Fatalf("managed update corrupted deployment: %+v", dep.Spec)
	}
	version := dep.ResourceVersion
	if err := r.ensureDeployment(context.Background(), ew, WorkloadInference); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), key, dep); err != nil {
		t.Fatal(err)
	}
	if version != dep.ResourceVersion {
		t.Fatal("unchanged template was written again")
	}
}

func TestBoundsConflictRetainsTargetAndFreezesDecisions(t *testing.T) {
	r, _ := newTestEnv(t, 210, nil)
	reconcileTestWorkload(t, r)
	ew := fetchTestWorkload(t, r)
	ew.Status.DesiredReplicas = aiinfrav1alpha1.WorkloadReplicas{Training: 3, Inference: 3}
	if err := r.Status().Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	ew.Spec.Training.MinReplicas = 4
	if err := r.Update(context.Background(), ew); err != nil {
		t.Fatal(err)
	}
	metrics := &countingMetrics{value: 600}
	r.Metrics = metrics
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}})
	if err == nil {
		t.Fatal("incompatible bounds accepted")
	}
	got := fetchTestWorkload(t, r)
	condition := apimeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if got.Status.DesiredReplicas.Training != 3 || got.Status.DesiredReplicas.Inference != 3 || metrics.calls != 0 ||
		condition == nil || condition.Reason != "InvalidAllocation" {
		t.Fatalf("invalid configuration was not frozen: %+v", got.Status)
	}
}
