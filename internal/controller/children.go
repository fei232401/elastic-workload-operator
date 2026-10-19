package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

func (r *ElasticWorkloadReconciler) getChild(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload, workload string) (client.Object, error) {
	var object client.Object = &appsv1.Deployment{}
	if workload == WorkloadTraining {
		object = &appsv1.StatefulSet{}
	}
	if err := r.Get(ctx, types.NamespacedName{Name: childName(ew, workload), Namespace: ew.Namespace}, object); err != nil {
		return nil, err
	}
	if !ownedBy(object, ew) {
		return nil, fmt.Errorf("%s/%s is not owned by this ElasticWorkload", ew.Namespace, object.GetName())
	}
	return object, nil
}

func childReplicas(object client.Object) *int32 {
	switch child := object.(type) {
	case *appsv1.StatefulSet:
		return child.Spec.Replicas
	case *appsv1.Deployment:
		return child.Spec.Replicas
	}
	return nil
}

func setChildReplicas(object client.Object, replicas int32) {
	switch child := object.(type) {
	case *appsv1.StatefulSet:
		child.Spec.Replicas = &replicas
	case *appsv1.Deployment:
		child.Spec.Replicas = &replicas
	}
}

func childReady(object client.Object) int32 {
	switch child := object.(type) {
	case *appsv1.StatefulSet:
		return child.Status.ReadyReplicas
	case *appsv1.Deployment:
		return child.Status.ReadyReplicas
	}
	return 0
}

func childAvailable(object client.Object, target int32) bool {
	switch child := object.(type) {
	case *appsv1.StatefulSet:
		return statefulSetAvailable(child, target)
	case *appsv1.Deployment:
		return deploymentAvailable(child, target)
	}
	return false
}

func childStalled(object client.Object) bool {
	deployment, ok := object.(*appsv1.Deployment)
	if !ok {
		return false
	}
	for _, condition := range deployment.Status.Conditions {
		if condition.Type == appsv1.DeploymentProgressing && condition.Status == corev1.ConditionFalse &&
			condition.Reason == "ProgressDeadlineExceeded" {
			return true
		}
	}
	return false
}

func (r *ElasticWorkloadReconciler) podCount(ctx context.Context, ew *aiinfrav1alpha1.ElasticWorkload, workload string) (int32, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(ew.Namespace), client.MatchingLabels(workloadLabels(ew.Name, workload))); err != nil {
		return 0, err
	}
	var count int32
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			count++
		}
	}
	return count, nil
}
