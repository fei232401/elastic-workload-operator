package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

func TestGeneratedCRDSchema(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "aiinfra.example.com_elasticworkloads.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var definition apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &definition); err != nil {
		t.Fatal(err)
	}
	var internal apiextensions.CustomResourceDefinition
	if err := apiextensionsv1.Convert_v1_CustomResourceDefinition_To_apiextensions_CustomResourceDefinition(&definition, &internal, nil); err != nil {
		t.Fatal(err)
	}
	internal.Status.StoredVersions = []string{"v1alpha1"}
	if errors := validation.ValidateCustomResourceDefinition(context.Background(), &internal); len(errors) != 0 {
		t.Fatalf("invalid generated CRD: %v", errors)
	}
	var structuralSource apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(definition.Spec.Versions[0].Schema.OpenAPIV3Schema, &structuralSource, nil); err != nil {
		t.Fatal(err)
	}
	structure, err := schema.NewStructural(&structuralSource)
	if err != nil {
		t.Fatal(err)
	}
	reconciler, _, _ := newHandoffEnv(t, 412)
	reconcileTestWorkload(t, reconciler)
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(fetchTestWorkload(t, reconciler))
	if err != nil {
		t.Fatal(err)
	}
	defaulting.Default(object, structure)
	snapshot := object["status"].(map[string]any)["handoff"].(map[string]any)["specSnapshot"]
	if !equality.Semantic.DeepEqual(object["spec"], snapshot) {
		t.Fatal("CRD defaults make the persisted snapshot differ from spec")
	}
}
