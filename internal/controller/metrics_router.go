package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// routerMetrics 是 MetricsReader 的真实现:HTTP GET KV Router(数据面)的 ttft_p95。
// 端点约定(数据面是我们自己的,格式自定,非行业标准):
//
//	GET {base}/ttft_p95 → 200 {"ttft_p95_ms": 412}
//
// 若将来接真实 Prometheus(查 promql),换一个实现即可,接口不变(面试点:依赖倒置)。
type routerMetrics struct {
	url    string
	client *http.Client
	policy MetricsPolicy
}

type MetricsPolicy struct {
	MaxAge        time.Duration
	MinSamples    int64
	MaxFutureSkew time.Duration
	AllowLegacy   bool
}

type metricValidationError struct {
	reason  string
	message string
}

func (e *metricValidationError) Error() string {
	return e.message
}

func invalidMetric(reason, message string) error {
	return &metricValidationError{reason: reason, message: message}
}

// NewRouterMetrics 构造读 KV Router 指标的 MetricsReader。
// timeout:读指标必须快,慢了会拖住 reconcile 循环(2s 足够集群内访问)。
func NewRouterMetrics(url string) MetricsReader {
	reader, _ := NewRouterMetricsWithPolicy(url, MetricsPolicy{
		MaxAge:        30 * time.Second,
		MinSamples:    20,
		MaxFutureSkew: 5 * time.Second,
	})
	return reader
}

func NewRouterMetricsWithPolicy(url string, policy MetricsPolicy) (MetricsReader, error) {
	if policy.MaxAge <= 0 || policy.MinSamples < 1 || policy.MaxFutureSkew < 0 {
		return nil, fmt.Errorf("metric policy requires positive max age and sample count, and non-negative clock tolerance")
	}
	return &routerMetrics{
		url:    strings.TrimRight(url, "/"),
		client: &http.Client{Timeout: 2 * time.Second},
		policy: policy,
	}, nil
}

type ttftResponse struct {
	TTFTP95MS   *int32     `json:"ttft_p95_ms"`
	ObservedAt  *time.Time `json:"observed_at"`
	SampleCount *int64     `json:"sample_count"`
}

func (m *routerMetrics) validateSample(out ttftResponse, now time.Time) error {
	if out.TTFTP95MS == nil || *out.TTFTP95MS < 0 {
		return invalidMetric("InvalidMetricValue", "ttft_p95_ms must be present and non-negative")
	}
	if out.ObservedAt == nil && out.SampleCount == nil && m.policy.AllowLegacy {
		return nil
	}
	if out.ObservedAt == nil || out.SampleCount == nil || out.ObservedAt.IsZero() {
		return invalidMetric("MetricMetadataMissing", "observed_at and sample_count are required")
	}
	if *out.SampleCount < m.policy.MinSamples {
		return invalidMetric("InsufficientSamples", fmt.Sprintf("metric has %d samples; need at least %d", *out.SampleCount, m.policy.MinSamples))
	}
	if out.ObservedAt.After(now.Add(m.policy.MaxFutureSkew)) {
		return invalidMetric("MetricClockSkew", "metric timestamp exceeds allowed future clock tolerance")
	}
	if now.Sub(*out.ObservedAt) > m.policy.MaxAge {
		return invalidMetric("MetricStale", "latest metric sample is older than the configured maximum age")
	}
	return nil
}

// TTFTP95MS 发 HTTP GET 读 ttft_p95。失败返回 error → Reconcile 退避重试,不崩。
func (m *routerMetrics) TTFTP95MS(ctx context.Context) (int32, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.url+"/ttft_p95", nil)
	if err != nil {
		return 0, err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("query router: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logf.FromContext(ctx).Error(closeErr, "Could not close metrics response")
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("router returned status %d", resp.StatusCode)
	}
	const maxResponseBytes = 64 * 1024
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return 0, fmt.Errorf("read router response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return 0, invalidMetric("MetricResponseTooLarge", fmt.Sprintf("router response exceeds %d bytes", maxResponseBytes))
	}
	var out ttftResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&out); err != nil {
		return 0, invalidMetric("InvalidMetricPayload", fmt.Sprintf("decode router response: %v", err))
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return 0, invalidMetric("InvalidMetricPayload", "router response must contain exactly one JSON object")
	}
	if err := m.validateSample(out, time.Now()); err != nil {
		return 0, err
	}
	return *out.TTFTP95MS, nil
}
