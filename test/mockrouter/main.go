// mockrouter 是 L2 演示用的"假数据面":站在 KV Router 的位置,按脚本吐 ttft_p95。
// 它不是真 KV Router(真数据面=KV 项目的 Python 服务,实测真实 vLLM 的 TTFT);
// 它只模仿 MetricsReader 读的那个指标端点,驱动算子的决策循环。
//
// 端点:
//
//	GET /ttft_p95          → {"ttft_p95_ms": <值>}
//	GET /control?ttft=600  → 手动覆盖 ttft(演示"我调高负载,算子开始让卡")
//	GET /control?auto=1    → 回到脚本曲线
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	start       = time.Now()
	mu          sync.Mutex
	manual      int32
	hasManual   bool
	sampleCount int64 = 100
	frozenAt    *time.Time
)

// scriptedTTFT 脚本曲线(三阶段演示全闭环):
//   - <20s  : 150ms → 死区 [180,300],不动
//   - <100s : 600ms → 过载 >300,让渡(持续 80s,配合 minHold=2tick 能看多步)
//   - <140s : 120ms → 健康 <180,归还
//   - 之后  : 150ms → 回稳
func scriptedTTFT() int32 {
	elapsed := int(time.Since(start).Seconds())
	switch {
	case elapsed < 20:
		return 150
	case elapsed < 100:
		return 600
	case elapsed < 140:
		return 120
	default:
		return 150
	}
}

func main() {
	if initial := os.Getenv("INITIAL_TTFT_MS"); initial != "" {
		value, err := strconv.ParseInt(initial, 10, 32)
		if err != nil || value < 0 {
			log.Fatal("INITIAL_TTFT_MS must be a non-negative int32")
		}
		manual, hasManual = int32(value), true
	}
	http.HandleFunc("/ttft_p95", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		value := scriptedTTFT()
		if hasManual {
			value = manual
		}
		observedAt := time.Now().UTC()
		if frozenAt != nil {
			observedAt = *frozenAt
		}
		count := sampleCount
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"ttft_p95_ms":  value,
			"observed_at":  observedAt,
			"sample_count": count,
			"source":       "mock",
		}); err != nil {
			log.Printf("Could not encode mock metric: %v", err)
		}
	})

	http.HandleFunc("/control", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if !q.Has("auto") && !q.Has("ttft") && !q.Has("samples") && !q.Has("freeze") {
			http.Error(w, "expected ttft, samples, freeze or auto", http.StatusBadRequest)
			return
		}
		var value int64
		var count int64
		var err error
		if q.Has("ttft") {
			value, err = strconv.ParseInt(q.Get("ttft"), 10, 32)
			if err != nil {
				http.Error(w, "ttft must be an int32", http.StatusBadRequest)
				return
			}
		}
		if q.Has("samples") {
			count, err = strconv.ParseInt(q.Get("samples"), 10, 64)
			if err != nil || count < 0 {
				http.Error(w, "samples must be a non-negative integer", http.StatusBadRequest)
				return
			}
		}
		if q.Has("freeze") && q.Get("freeze") != "0" && q.Get("freeze") != "1" {
			http.Error(w, "freeze must be 0 or 1", http.StatusBadRequest)
			return
		}
		if q.Has("auto") && q.Get("auto") != "1" {
			http.Error(w, "auto must be 1", http.StatusBadRequest)
			return
		}
		mu.Lock()
		if q.Get("auto") == "1" {
			hasManual, sampleCount, frozenAt = false, 100, nil
		}
		if q.Has("ttft") {
			manual, hasManual = int32(value), true
		}
		if q.Has("samples") {
			sampleCount = count
		}
		if q.Get("freeze") == "1" && frozenAt == nil {
			stamp := time.Now().UTC()
			frozenAt = &stamp
		} else if q.Get("freeze") == "0" {
			frozenAt = nil
		}
		mu.Unlock()
		if _, err := fmt.Fprintln(w, "mock controls updated"); err != nil {
			log.Printf("Could not write mock response: %v", err)
		}
	})

	addr := ":8080"
	if a := os.Getenv("ADDR"); a != "" {
		addr = a
	}
	fmt.Printf("mock router listening on %s\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
