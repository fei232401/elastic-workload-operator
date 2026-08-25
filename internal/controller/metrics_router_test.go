package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouterMetrics_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ttft_p95" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(map[string]int32{"ttft_p95_ms": 412}); err != nil {
			t.Fatal(err)
		}
	}))
	defer srv.Close()

	m := NewRouterMetrics(srv.URL)
	got, err := m.TTFTP95MS(context.Background())
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
	if _, err := m.TTFTP95MS(context.Background()); err == nil {
		t.Fatal("want error on non-200")
	}
}

func TestRouterMetrics_Unreachable(t *testing.T) {
	m := NewRouterMetrics("http://127.0.0.1:1") // 必有失败端口
	if _, err := m.TTFTP95MS(context.Background()); err == nil {
		t.Fatal("want error on unreachable router")
	}
}
