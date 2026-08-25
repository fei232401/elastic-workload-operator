/*
Copyright 2026 fei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ElasticWorkloadSpec defines the desired state of ElasticWorkload
type ElasticWorkloadSpec struct {
	// Training workload: the long-running batch job (训练侧)
	// +kubebuilder:validation:XValidation:rule="self.minReplicas <= self.replicas && self.replicas <= self.maxReplicas",message="training: minReplicas <= replicas <= maxReplicas"
	Training TrainingSpec `json:"training"`

	// Inference workload: serving side backed by KV data plane (推理侧,数据面=KV 路由+vLLM)
	// +kubebuilder:validation:XValidation:rule="self.minReplicas <= self.replicas && self.replicas <= self.maxReplicas",message="inference: minReplicas <= replicas <= maxReplicas"
	Inference InferenceSpec `json:"inference"`

	// ScalingPolicy: 项目一 Rule1-5 的配置(参数全部来自项目一已交付证据,无新参数)
	// +kubebuilder:validation:XValidation:rule="self.slo > 0",message="slo must be > 0"
	// +kubebuilder:validation:XValidation:rule="self.minHoldDuration >= 0",message="minHoldDuration must be >= 0"
	// +kubebuilder:validation:XValidation:rule="self.maxScaleStep >= 1",message="maxScaleStep must be >= 1"
	ScalingPolicy ScalingPolicy `json:"scalingPolicy"`
}

// TrainingSpec defines the training workload (long-running batch job)
type TrainingSpec struct {
	// Desired number of training workers
	Replicas int32 `json:"replicas"`
	// Floor: elastic scaling must not go below this (弹性下限,不能把训练清零)
	MinReplicas int32 `json:"minReplicas"`
	// Ceiling
	MaxReplicas int32 `json:"maxReplicas"`
	// GPUs per worker (项目一 scale law: 吞吐 ∝ g^0.85)
	GPUPerWorker int32 `json:"gpuPerWorker"`
	// Container image
	Image string `json:"image"`
	// Entrypoint command
	Command []string `json:"command,omitempty"`
}

// InferenceSpec defines the inference workload (serving, backed by KV data plane)
type InferenceSpec struct {
	// Desired number of vLLM instances
	Replicas int32 `json:"replicas"`
	// Floor
	MinReplicas int32 `json:"minReplicas"`
	// Ceiling
	MaxReplicas int32 `json:"maxReplicas"`
	// GPUs per instance (util=0.35, 4090 校准:多实例近免费)
	GPUPerInstance int32 `json:"gpuPerInstance"`
	// Container image (vLLM)
	Image string `json:"image"`
	// Enable prefix caching (KV 数据面核心:前缀命中省 prefill)
	PrefixCaching bool `json:"prefixCaching,omitempty"`
	// Entrypoint command (vLLM 实例启动命令;L2 mock 用它跑 busybox)
	Command []string `json:"command,omitempty"`
}

// ScalingPolicy is the config of 项目一 Rule1-5 (参数来源全部为项目一已交付证据)
type ScalingPolicy struct {
	// Target metric (目前只支持 ttft_p95)
	// +kubebuilder:validation:Enum=ttft_p95
	TargetMetric string `json:"targetMetric"`
	// TTFT P95 SLO threshold in ms (项目一:300ms)
	SLO int32 `json:"slo"`
	// Min hold duration in ticks after a scaling action (@5s = 120s,项目一 Rule4)
	MinHoldDuration int32 `json:"minHoldDuration"`
	// Max replicas change per decision cycle (渐进 ±1,项目一 Rule5)
	MaxScaleStep int32 `json:"maxScaleStep"`
}

// ElasticWorkloadStatus defines the observed state of ElasticWorkload.
type ElasticWorkloadStatus struct {
	// Phase: Pending / Running / Deleting
	Phase string `json:"phase,omitempty"`
	// Observed ready replicas of both workloads
	ReadyReplicas WorkloadReplicas `json:"readyReplicas,omitempty"`
	// DesiredReplicas is the controller's committed target allocation
	// (single source of truth: apply converges child Deployments toward it).
	// 幕8 声明式目标收敛:决策先原子提交意图到 status,apply 朝它收敛,
	// 部分失败重试时不叠加漂移(修复旧两段式写的 6→5 丢卡)。
	DesiredReplicas WorkloadReplicas `json:"desiredReplicas,omitempty"`
	// conditions represent the current state of the ElasticWorkload resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Recent scaling decisions (audit trail,对应项目一决策日志回放)
	ScalingHistory []ScalingEvent `json:"scalingHistory,omitempty"`
}

// WorkloadReplicas holds ready replica counts for both workloads
type WorkloadReplicas struct {
	Training  int32 `json:"training"`
	Inference int32 `json:"inference"`
}

// ScalingEvent records one scaling decision (项目一决策日志的 K8s 版)
type ScalingEvent struct {
	// When the decision was applied
	Time metav1.Time `json:"time"`
	// e.g. "inference+1/training-1"
	Action string `json:"action"`
	// e.g. "overload" / "recovery"
	Reason string `json:"reason"`
	// e.g. "ttft_p95=412ms"
	Metric string `json:"metric,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.inference.replicas,statuspath=.status.readyReplicas.inference

// ElasticWorkload is the Schema for the elasticworkloads API
type ElasticWorkload struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ElasticWorkload
	// +required
	Spec ElasticWorkloadSpec `json:"spec"`

	// status defines the observed state of ElasticWorkload
	// +optional
	Status ElasticWorkloadStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ElasticWorkloadList contains a list of ElasticWorkload
type ElasticWorkloadList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ElasticWorkload `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &ElasticWorkload{}, &ElasticWorkloadList{})
		return nil
	})
}
