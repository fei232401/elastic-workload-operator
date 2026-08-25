package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
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
}

// NewRouterMetrics 构造读 KV Router 指标的 MetricsReader。
// timeout:读指标必须快,慢了会拖住 reconcile 循环(2s 足够集群内访问)。
func NewRouterMetrics(url string) MetricsReader {
	return &routerMetrics{
		url:    url,
		client: &http.Client{Timeout: 2 * time.Second},
	}
}

type ttftResponse struct {
	TTFTP95MS int32 `json:"ttft_p95_ms"`
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("router returned status %d", resp.StatusCode)
	}
	var out ttftResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("decode router response: %w", err)
	}
	return out.TTFTP95MS, nil
}
