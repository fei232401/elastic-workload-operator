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

package controller

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

// envtest 集成测试(L1.5):真 apiserver + 假数据面(fakeMetrics)。
// 全链路 Reconcile:ensure 子 Deployment → observe → decide → apply → finalizer/phase。
var _ = Describe("ElasticWorkload Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)
		ctx := context.Background()
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			By("creating a valid ElasticWorkload(过载场景:ttft=412 > SLO=300)")
			resource := &aiinfrav1alpha1.ElasticWorkload{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: aiinfrav1alpha1.ElasticWorkloadSpec{
					Training: aiinfrav1alpha1.TrainingSpec{
						Replicas: 4, MinReplicas: 2, MaxReplicas: 8, GPUPerWorker: 1,
						Image: "training-img:v1", Command: []string{"/train.sh"}},
					Inference: aiinfrav1alpha1.InferenceSpec{
						Replicas: 2, MinReplicas: 1, MaxReplicas: 4, GPUPerInstance: 1,
						Image: "vllm-img:v1", PrefixCaching: true},
					ScalingPolicy: aiinfrav1alpha1.ScalingPolicy{
						TargetMetric: "ttft_p95", SLO: 300, MinHoldDuration: 24, MaxScaleStep: 1},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			By("cleanup: 移除 finalizer 让删除真正生效(controller 未常驻,手动 finalize)")
			resource := &aiinfrav1alpha1.ElasticWorkload{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if errors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())
			if !resource.DeletionTimestamp.IsZero() {
				controllerutil.RemoveFinalizer(resource, FinalizerName)
				Expect(k8sClient.Update(ctx, resource)).To(Succeed())
			} else {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("全链路:过载让渡 训练4/推理2 → 3/3,finalizer+phase=Running+审计事件", func() {
			By("reconciling with fake metrics ttft=412(过载)")
			controllerReconciler := &ElasticWorkloadReconciler{
				Client:  k8sClient,
				Scheme:  k8sClient.Scheme(),
				Metrics: &fakeMetrics{ttft: 412},
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("asserting 子 Deployment:训练 4→3、推理 2→3")
			training := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-training", Namespace: resourceNamespace}, training)).To(Succeed())
			Expect(*training.Spec.Replicas).To(Equal(int32(3)))
			inference := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-inference", Namespace: resourceNamespace}, inference)).To(Succeed())
			Expect(*inference.Spec.Replicas).To(Equal(int32(3)))

			By("asserting finalizer + phase=Running + 审计事件")
			got := &aiinfrav1alpha1.ElasticWorkload{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, got)).To(Succeed())
			Expect(got.Finalizers).To(ContainElement(FinalizerName))
			Expect(got.Status.Phase).To(Equal("Running"))
			Expect(got.Status.ScalingHistory).To(HaveLen(1))
			Expect(got.Status.ScalingHistory[0].Reason).To(Equal("overload"))
		})
	})
})
