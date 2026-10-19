package controller

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

const (
	checkpointMountPath      = "/ckpt"
	checkpointVolumeName     = "checkpoint"
	defaultCheckpointPort    = 8080
	defaultRendezvousPort    = 29400
	defaultCheckpointTimeout = 60
)

func childName(ew *aiinfrav1alpha1.ElasticWorkload, workload string) string {
	return ew.Name + "-" + workload
}

func checkpointSettings(ew *aiinfrav1alpha1.ElasticWorkload) (port, rendezvous, timeout int32) {
	port, rendezvous, timeout = defaultCheckpointPort, defaultRendezvousPort, defaultCheckpointTimeout
	if spec := ew.Spec.Training.Checkpoint; spec != nil {
		if spec.Port > 0 {
			port = spec.Port
		}
		if spec.RendezvousPort > 0 {
			rendezvous = spec.RendezvousPort
		}
		if spec.TimeoutSeconds > 0 {
			timeout = spec.TimeoutSeconds
		}
	}
	return port, rendezvous, timeout
}

func checkpointEndpoint(ew *aiinfrav1alpha1.ElasticWorkload) string {
	port, _, _ := checkpointSettings(ew)
	name := childName(ew, WorkloadTraining)
	return fmt.Sprintf("http://%s-0.%s.%s.svc:%d", name, name, ew.Namespace, port)
}

func trainingEnv(ew *aiinfrav1alpha1.ElasticWorkload) []corev1.EnvVar {
	port, rendezvous, _ := checkpointSettings(ew)
	name := childName(ew, WorkloadTraining)
	env := []corev1.EnvVar{
		{Name: "RDZV_ENDPOINT", Value: fmt.Sprintf("%s-0.%s:%d", name, name, rendezvous)},
		{Name: "RDZV_ID", Value: string(ew.UID)},
		{Name: "MIN_NODES", Value: strconv.Itoa(int(max(1, ew.Spec.Training.MinReplicas)))},
		{Name: "MAX_NODES", Value: strconv.Itoa(int(max(1, ew.Spec.Training.MaxReplicas)))},
		{Name: "CONTROL_PORT", Value: strconv.Itoa(int(port))},
	}
	if spec := ew.Spec.Training.Checkpoint; spec != nil && spec.ClaimName != "" {
		env = append(env, corev1.EnvVar{Name: "CHECKPOINT_DIR", Value: checkpointMountPath})
	}
	return env
}

var managedTrainingEnv = []string{"RDZV_ENDPOINT", "RDZV_ID", "MIN_NODES", "MAX_NODES", "CONTROL_PORT", "CHECKPOINT_DIR"}

func upsertEnv(existing, managed []corev1.EnvVar) []corev1.EnvVar {
	wanted := map[string]bool{}
	for _, variable := range managed {
		wanted[variable.Name] = true
	}
	result := make([]corev1.EnvVar, 0, len(existing)+len(managed))
	for _, variable := range existing {
		if wanted[variable.Name] || !slices.Contains(managedTrainingEnv, variable.Name) {
			result = append(result, variable)
		}
	}
	for _, wanted := range managed {
		found := false
		for index := range result {
			if result[index].Name == wanted.Name {
				result[index] = wanted
				found = true
			}
		}
		if !found {
			result = append(result, wanted)
		}
	}
	return result
}

func trainingProbe(ew *aiinfrav1alpha1.ElasticWorkload) *corev1.Probe {
	if ew.Spec.Training.Checkpoint == nil {
		return nil
	}
	port, _, _ := checkpointSettings(ew)
	return &corev1.Probe{
		ProbeHandler:   corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(port), Scheme: corev1.URISchemeHTTP}},
		TimeoutSeconds: 1, PeriodSeconds: 2, SuccessThreshold: 1, FailureThreshold: 3,
	}
}

func checkpointVolume(ew *aiinfrav1alpha1.ElasticWorkload) (*corev1.Volume, *corev1.VolumeMount) {
	spec := ew.Spec.Training.Checkpoint
	if spec == nil || spec.ClaimName == "" {
		return nil, nil
	}
	return &corev1.Volume{
			Name:         checkpointVolumeName,
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: spec.ClaimName}},
		}, &corev1.VolumeMount{
			Name: checkpointVolumeName, MountPath: checkpointMountPath,
		}
}

func (r *ElasticWorkloadReconciler) desiredTraining(ew *aiinfrav1alpha1.ElasticWorkload) (*appsv1.StatefulSet, *corev1.Service, error) {
	container, err := managedContainer(ew, WorkloadTraining)
	if err != nil {
		return nil, nil, err
	}
	name := childName(ew, WorkloadTraining)
	labels := workloadLabels(ew.Name, WorkloadTraining)
	replicas := initialReplicas(ew, WorkloadTraining)
	port, rendezvous, _ := checkpointSettings(ew)
	pod := corev1.PodSpec{Containers: []corev1.Container{container}}
	if volume, mount := checkpointVolume(ew); volume != nil {
		pod.Volumes = []corev1.Volume{*volume}
		pod.Containers[0].VolumeMounts = []corev1.VolumeMount{*mount}
	}
	set := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ew.Namespace, Labels: labels},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            &replicas,
			ServiceName:         name,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: labels},
			Template:            corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: pod},
		},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ew.Namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 labels,
			Ports: []corev1.ServicePort{
				{Name: "rendezvous", Protocol: corev1.ProtocolTCP, Port: rendezvous, TargetPort: intstr.FromInt32(rendezvous)},
				{Name: "control", Protocol: corev1.ProtocolTCP, Port: port, TargetPort: intstr.FromInt32(port)},
			},
		},
	}
	for _, object := range []client.Object{set, service} {
		if err := controllerutil.SetControllerReference(ew, object, r.Scheme); err != nil {
			return nil, nil, err
		}
	}
	return set, service, nil
}

func (r *ElasticWorkloadReconciler) ensureTraining(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) error {
	desired, service, err := r.desiredTraining(ew)
	if err != nil {
		return err
	}
	if err := r.ensureTrainingService(ctx, ew, service); err != nil {
		return err
	}
	if err := r.removeLegacyTrainingDeployment(ctx, ew); err != nil {
		return err
	}
	var existing appsv1.StatefulSet
	err = r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: ew.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if !ownedBy(&existing, ew) {
		return fmt.Errorf("statefulset %s/%s exists but is not owned by this ElasticWorkload; refusing to adopt", ew.Namespace, desired.Name)
	}
	wanted := desired.Spec.Template.Spec.Containers[0]
	template := &existing.Spec.Template.Spec
	for index := range template.Containers {
		container := &template.Containers[index]
		if container.Name != WorkloadTraining {
			continue
		}
		before := container.DeepCopy()
		volumesBefore := append([]corev1.Volume(nil), template.Volumes...)
		container.Image = wanted.Image
		container.Command = wanted.Command
		container.Resources = wanted.Resources
		container.ReadinessProbe = wanted.ReadinessProbe
		container.Env = upsertEnv(container.Env, wanted.Env)
		volume, mount := checkpointVolume(ew)
		template.Volumes = replaceVolume(template.Volumes, volume)
		container.VolumeMounts = replaceMount(container.VolumeMounts, mount)
		if equality.Semantic.DeepEqual(before, container) && equality.Semantic.DeepEqual(volumesBefore, template.Volumes) {
			return nil
		}
		return r.Update(ctx, &existing)
	}
	return fmt.Errorf("owned statefulset %s has no managed container %s", existing.Name, WorkloadTraining)
}

func (r *ElasticWorkloadReconciler) removeLegacyTrainingDeployment(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload) error {
	var legacy appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Name: childName(ew, WorkloadTraining), Namespace: ew.Namespace}, &legacy)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !ownedBy(&legacy, ew) {
		return fmt.Errorf("deployment %s/%s is not owned by this ElasticWorkload; refusing to delete", ew.Namespace, legacy.Name)
	}
	return client.IgnoreNotFound(r.Delete(ctx, &legacy, client.PropagationPolicy(metav1.DeletePropagationForeground)))
}

func (r *ElasticWorkloadReconciler) ensureTrainingService(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload, desired *corev1.Service) error {
	var existing corev1.Service
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: ew.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if !ownedBy(&existing, ew) {
		return fmt.Errorf("service %s/%s exists but is not owned by this ElasticWorkload; refusing to adopt", ew.Namespace, desired.Name)
	}
	if equality.Semantic.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) && existing.Spec.PublishNotReadyAddresses {
		return nil
	}
	existing.Spec.Ports = desired.Spec.Ports
	existing.Spec.PublishNotReadyAddresses = true
	return r.Update(ctx, &existing)
}

func replaceVolume(volumes []corev1.Volume, wanted *corev1.Volume) []corev1.Volume {
	result := make([]corev1.Volume, 0, len(volumes)+1)
	for _, volume := range volumes {
		if volume.Name != checkpointVolumeName {
			result = append(result, volume)
		}
	}
	if wanted != nil {
		result = append(result, *wanted)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func replaceMount(mounts []corev1.VolumeMount, wanted *corev1.VolumeMount) []corev1.VolumeMount {
	result := make([]corev1.VolumeMount, 0, len(mounts)+1)
	for _, mount := range mounts {
		if mount.Name != checkpointVolumeName {
			result = append(result, mount)
		}
	}
	if wanted != nil {
		result = append(result, *wanted)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func statefulSetAvailable(set *appsv1.StatefulSet, target int32) bool {
	return set.Spec.Replicas != nil && *set.Spec.Replicas == target &&
		set.Status.ObservedGeneration >= set.Generation &&
		set.Status.Replicas == target && set.Status.UpdatedReplicas == target &&
		set.Status.ReadyReplicas == target && set.Status.AvailableReplicas == target
}

func ownedBy(object metav1.Object, ew *aiinfrav1alpha1.ElasticWorkload) bool {
	for _, ref := range object.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller && ref.UID == ew.UID {
			return true
		}
	}
	return false
}
