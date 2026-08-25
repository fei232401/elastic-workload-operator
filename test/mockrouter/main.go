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
	"net/http"
	"os"
	"sync"
	"time"
)

var (
	start      = time.Now()
	mu         sync.Mutex
	manual     int32
	hasManual  bool
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
	http.HandleFunc("/ttft_p95", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		v := int32(150)
		if hasManual {
			v = manual
		} else {
			v = scriptedTTFT()
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]int32{"ttft_p95_ms": v})
	})

	http.HandleFunc("/control", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		q := r.URL.Query()
		if q.Get("auto") == "1" {
			hasManual = false
			fmt.Fprintln(w, "back to scripted curve")
			return
		}
		var v int32
		if _, err := fmt.Sscanf(q.Get("ttft"), "%d", &v); err != nil {
			http.Error(w, "usage: /control?ttft=<ms> 或 /control?auto=1", http.StatusBadRequest)
			return
		}
		manual, hasManual = v, true
		fmt.Fprintf(w, "manual ttft set to %dms\n", v)
	})

	addr := ":8080"
	if a := os.Getenv("ADDR"); a != "" {
		addr = a
	}
	fmt.Printf("mock router listening on %s\n", addr)
	_ = http.ListenAndServe(addr, nil)
}
