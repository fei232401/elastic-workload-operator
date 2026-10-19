package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	aiinfrav1alpha1 "github.com/fei/elastic-workload-operator/api/v1alpha1"
)

func TestMetricValidation_HTTPContract(t *testing.T) {
	now := time.Now().UTC()
	fresh := now.Format(time.RFC3339Nano)
	stale := now.Add(-time.Hour).Format(time.RFC3339Nano)
	future := now.Add(time.Hour).Format(time.RFC3339Nano)
	cases := []struct {
		name, body, reason string
		legacy             bool
	}{
		{"valid", fmt.Sprintf(`{"ttft_p95_ms":412,"observed_at":%q,"sample_count":100}`, fresh), "", false},
		{"explicit_zero", fmt.Sprintf(`{"ttft_p95_ms":0,"observed_at":%q,"sample_count":20}`, fresh), "", false},
		{"missing_value", `{}`, "InvalidMetricValue", false},
		{"null_value", `{"ttft_p95_ms":null}`, "InvalidMetricValue", false},
		{"negative_value", `{"ttft_p95_ms":-1}`, "InvalidMetricValue", false},
		{"missing_metadata", `{"ttft_p95_ms":412}`, "MetricMetadataMissing", false},
		{"no_samples", fmt.Sprintf(`{"ttft_p95_ms":1,"observed_at":%q,"sample_count":0}`, fresh), "InsufficientSamples", false},
		{"few_samples", fmt.Sprintf(`{"ttft_p95_ms":1,"observed_at":%q,"sample_count":19}`, fresh), "InsufficientSamples", false},
		{"stale", fmt.Sprintf(`{"ttft_p95_ms":1,"observed_at":%q,"sample_count":100}`, stale), "MetricStale", false},
		{"future", fmt.Sprintf(`{"ttft_p95_ms":1,"observed_at":%q,"sample_count":100}`, future), "MetricClockSkew", false},
		{"bad_timestamp", `{"ttft_p95_ms":1,"observed_at":"yesterday","sample_count":100}`, "InvalidMetricPayload", false},
		{"multiple_values", `{}` + `{}`, "InvalidMetricPayload", false},
		{"invalid_json", `{`, "InvalidMetricPayload", false},
		{"fractional_value", `{"ttft_p95_ms":1.5}`, "InvalidMetricPayload", false},
		{"oversized", strings.Repeat(" ", 65*1024), "MetricResponseTooLarge", false},
		{"legacy_explicit", `{"ttft_p95_ms":412}`, "", true},
		{"legacy_partial_metadata", `{"ttft_p95_ms":412,"sample_count":100}`, "MetricMetadataMissing", true},
		{"legacy_still_checks_age", fmt.Sprintf(`{"ttft_p95_ms":1,"observed_at":%q,"sample_count":100}`, stale), "MetricStale", true},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(item.body))
			}))
			defer server.Close()
			reader, err := NewRouterMetricsWithPolicy(server.URL, MetricsPolicy{
				MaxAge: 30 * time.Second, MinSamples: 20, MaxFutureSkew: 5 * time.Second, AllowLegacy: item.legacy,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = reader.TTFTP95MS(context.Background(), aiinfrav1alpha1.MetricSource{})
			if item.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var validation *metricValidationError
			if !errors.As(err, &validation) || validation.reason != item.reason {
				t.Fatalf("want %s, got %v", item.reason, err)
			}
		})
	}
}
