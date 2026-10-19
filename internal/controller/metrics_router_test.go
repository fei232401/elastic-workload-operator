package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

func TestRouterMetrics_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ttft_p95" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(map[string]any{
			"ttft_p95_ms":  412,
			"observed_at":  time.Now().UTC(),
			"sample_count": 100,
		}); err != nil {
			t.Fatal(err)
		}
	}))
	defer srv.Close()

	m := NewRouterMetrics(srv.URL)
	got, err := m.TTFTP95MS(context.Background(), aiinfrav1alpha1.MetricSource{})
	if err != nil {
		t.Fatal(err)
	}
	if got != 412 {
		t.Fatalf("want 412, got %d", got)
	}
}

func TestRouterMetrics_NonOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	m := NewRouterMetrics(srv.URL)
	if _, err := m.TTFTP95MS(context.Background(), aiinfrav1alpha1.MetricSource{}); err == nil {
		t.Fatal("want error on non-200")
	}
}

func TestRouterMetrics_Unreachable(t *testing.T) {
	m := NewRouterMetrics("http://127.0.0.1:1")
	if _, err := m.TTFTP95MS(context.Background(), aiinfrav1alpha1.MetricSource{}); err == nil {
		t.Fatal("want error on unreachable router")
	}
}

func modelServer(t *testing.T, echo func(requested string) any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]any{
			"ttft_p95_ms":  250,
			"observed_at":  time.Now().UTC(),
			"sample_count": 100,
			"model":        echo(r.URL.Query().Get("model")),
		}); err != nil {
			t.Fatal(err)
		}
	}))
}

func TestRouterMetrics_WorkloadSourceOverridesDefault(t *testing.T) {
	var requested string
	srv := modelServer(t, func(model string) any {
		requested = model
		return model
	})
	defer srv.Close()

	m := NewRouterMetrics("http://127.0.0.1:1")
	got, err := m.TTFTP95MS(context.Background(), aiinfrav1alpha1.MetricSource{RouterURL: srv.URL + "/", Model: "qwen 3b"})
	if err != nil {
		t.Fatal(err)
	}
	if got != 250 || requested != "qwen 3b" {
		t.Fatalf("want 250 for model %q, got %d for %q", "qwen 3b", got, requested)
	}
}

func TestRouterMetrics_ModelMismatch(t *testing.T) {
	for name, echo := range map[string]func(string) any{
		"other_model": func(string) any { return "default-model" },
		"no_model":    func(string) any { return nil },
	} {
		t.Run(name, func(t *testing.T) {
			srv := modelServer(t, echo)
			defer srv.Close()
			_, err := NewRouterMetrics(srv.URL).TTFTP95MS(context.Background(), aiinfrav1alpha1.MetricSource{Model: "qwen-3b"})
			var validation *metricValidationError
			if !errors.As(err, &validation) || validation.reason != "MetricModelMismatch" {
				t.Fatalf("want MetricModelMismatch, got %v", err)
			}
		})
	}
}
