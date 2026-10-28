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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ElasticWorkloadSpec declares a training workload and an inference workload that share a replica budget.
// +kubebuilder:validation:XValidation:rule="self.training.gpuPerWorker == self.inference.gpuPerInstance",message="only equal GPU units per replica are supported"
type ElasticWorkloadSpec struct {
	// Training is the preemptible workload that lends replicas to inference under load.
	// +kubebuilder:validation:XValidation:rule="self.minReplicas <= self.replicas && self.replicas <= self.maxReplicas",message="training: minReplicas <= replicas <= maxReplicas"
	Training TrainingSpec `json:"training"`

	// Inference is the latency-sensitive serving workload.
	// +kubebuilder:validation:XValidation:rule="self.minReplicas <= self.replicas && self.replicas <= self.maxReplicas",message="inference: minReplicas <= replicas <= maxReplicas"
	Inference InferenceSpec `json:"inference"`

	// ScalingPolicy controls when replicas move between the two workloads.
	// +kubebuilder:validation:XValidation:rule="self.slo > 0",message="slo must be > 0"
	// +kubebuilder:validation:XValidation:rule="self.minHoldDuration >= 0",message="minHoldDuration must be >= 0"
	// +kubebuilder:validation:XValidation:rule="self.maxScaleStep >= 1",message="maxScaleStep must be >= 1"
	ScalingPolicy ScalingPolicy `json:"scalingPolicy"`
}

// TrainingSpec defines the training workload.
type TrainingSpec struct {
	// Replicas is the initial number of training workers.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="initial replicas are immutable; recreate the workload to change its budget"
	Replicas int32 `json:"replicas"`
	// MinReplicas is the lowest training replica count the controller may set.
	MinReplicas int32 `json:"minReplicas"`
	// MaxReplicas is the highest training replica count the controller may set.
	MaxReplicas int32 `json:"maxReplicas"`
	// GPUPerWorker is the nvidia.com/gpu count requested by each worker; zero requests no GPU.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="GPU units are immutable"
	GPUPerWorker int32             `json:"gpuPerWorker"`
	Image        string            `json:"image"`
	Command      []string          `json:"command,omitempty"`
	Resources    WorkloadResources `json:"resources,omitempty"`
	// Checkpoint enables the checkpoint handshake before training membership changes.
	// +optional
	Checkpoint *CheckpointSpec `json:"checkpoint,omitempty"`
}

// CheckpointSpec describes how the controller asks training workers to persist progress.
type CheckpointSpec struct {
	// Port serves POST /checkpoint?id=N and GET /checkpoint on every training pod.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=8080
	// +optional
	Port int32 `json:"port,omitempty"`
	// RendezvousPort is the torchrun c10d rendezvous port hosted by ordinal 0.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=29400
	// +optional
	RendezvousPort int32 `json:"rendezvousPort,omitempty"`
	// TimeoutSeconds bounds the wait for a requested checkpoint before the handoff proceeds without it.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=60
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
	// ClaimName is an existing PersistentVolumeClaim mounted at /ckpt in every training pod.
	// All ranks must share it so that any pod can report the latest checkpoint.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ClaimName string `json:"claimName"`
}

// InferenceSpec defines the inference workload.
type InferenceSpec struct {
	// Replicas is the initial number of inference instances.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="initial replicas are immutable; recreate the workload to change its budget"
	Replicas int32 `json:"replicas"`
	// MinReplicas is the lowest inference replica count the controller may set.
	MinReplicas int32 `json:"minReplicas"`
	// MaxReplicas is the highest inference replica count the controller may set.
	MaxReplicas int32 `json:"maxReplicas"`
	// GPUPerInstance is the nvidia.com/gpu count requested by each instance; zero requests no GPU.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="GPU units are immutable"
	GPUPerInstance int32  `json:"gpuPerInstance"`
	Image          string `json:"image"`
	// PrefixCaching is rejected by validation; engine flags belong in Command.
	PrefixCaching bool              `json:"prefixCaching,omitempty"`
	Command       []string          `json:"command,omitempty"`
	Resources     WorkloadResources `json:"resources,omitempty"`
	ReadinessHTTP *HTTPHealthCheck  `json:"readinessHTTP,omitempty"`
}

type HTTPHealthCheck struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
	// +kubebuilder:validation:Pattern="^/"
	// +kubebuilder:default="/health"
	Path string `json:"path,omitempty"`
}

type WorkloadResources struct {
	Requests corev1.ResourceList `json:"requests,omitempty"`
	Limits   corev1.ResourceList `json:"limits,omitempty"`
}

// ScalingPolicy moves replicas to inference when TTFT P95 exceeds the SLO and returns them below 60% of it.
type ScalingPolicy struct {
	// +kubebuilder:validation:Enum=ttft_p95
	TargetMetric string `json:"targetMetric"`
	// SLO is the TTFT P95 threshold in milliseconds.
	SLO int32 `json:"slo"`
	// MinHoldDuration is the number of 5s ticks to wait after a scaling action before deciding again.
	MinHoldDuration int32 `json:"minHoldDuration"`
	// MaxScaleStep is the largest replica change per decision.
	MaxScaleStep int32 `json:"maxScaleStep"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=300
	// +optional
	HandoffTimeoutSeconds int32 `json:"handoffTimeoutSeconds,omitempty"`
	// Metrics selects the TTFT source for this workload; unset fields fall back to the controller defaults.
	// +optional
	Metrics *MetricSource `json:"metrics,omitempty"`
}

// MetricSource identifies where a workload's TTFT P95 is read from.
type MetricSource struct {
	// RouterURL overrides the controller's --router-url for this workload.
	// +kubebuilder:validation:Pattern=`^https?://`
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	RouterURL string `json:"routerURL,omitempty"`
	// Model is the logical model whose TTFT window is requested; the router must echo it back.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=200
	// +optional
	Model string `json:"model,omitempty"`
}

// ElasticWorkloadStatus defines the observed state of ElasticWorkload.
type ElasticWorkloadStatus struct {
	// Phase is one of Pending, Progressing, Running, Degraded or Deleting.
	Phase string `json:"phase,omitempty"`
	// ReadyReplicas is the observed ready replica count of each child Deployment.
	ReadyReplicas WorkloadReplicas `json:"readyReplicas,omitempty"`
	// DesiredReplicas is the committed target allocation that child Deployments converge to.
	DesiredReplicas WorkloadReplicas `json:"desiredReplicas,omitempty"`
	// Conditions include Initialized, Ready, Progressing and MetricsReady.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// ScalingHistory keeps the most recent committed decisions.
	ScalingHistory []ScalingEvent `json:"scalingHistory,omitempty"`
	// LastCheckpointID is the highest checkpoint request identifier issued so far.
	// +optional
	LastCheckpointID int64 `json:"lastCheckpointID,omitempty"`
	// Handoff tracks an in-progress checkpointed replica transfer.
	// +optional
	Handoff *Handoff `json:"handoff,omitempty"`
}

// Handoff moves replicas in three phases: CheckpointRequested, Releasing and Acquiring.
type Handoff struct {
	// +kubebuilder:validation:Enum=CheckpointRequested;Releasing;Acquiring
	Phase string           `json:"phase"`
	From  WorkloadReplicas `json:"from"`
	To    WorkloadReplicas `json:"to"`
	// CheckpointID is the monotonically increasing request identifier sent to the trainers.
	CheckpointID int64       `json:"checkpointID"`
	RequestedAt  metav1.Time `json:"requestedAt"`
	// +optional
	PhaseStartedAt *metav1.Time `json:"phaseStartedAt,omitempty"`
	// +optional
	SpecSnapshot *ElasticWorkloadSpec `json:"specSnapshot,omitempty"`
	// +optional
	CheckpointedAt *metav1.Time `json:"checkpointedAt,omitempty"`
	// +optional
	CheckpointStep int64 `json:"checkpointStep,omitempty"`
	// +optional
	CheckpointTimedOut bool `json:"checkpointTimedOut,omitempty"`
	// +optional
	ReleasedAt *metav1.Time `json:"releasedAt,omitempty"`
}

// WorkloadReplicas holds a replica count for each workload.
type WorkloadReplicas struct {
	Training  int32 `json:"training"`
	Inference int32 `json:"inference"`
}

// ScalingEvent records one committed scaling decision.
type ScalingEvent struct {
	Time metav1.Time `json:"time"`
	// Action is the replica delta, for example "inference+1/training-1".
	Action string `json:"action"`
	// Reason is overload or recovery.
	Reason string `json:"reason"`
	// Metric is the observation that triggered the decision, for example "ttft_p95=412ms".
	Metric string `json:"metric,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

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
