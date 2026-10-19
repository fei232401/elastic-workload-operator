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
		model := r.URL.Query().Get("model")
		if model == "" {
			model = os.Getenv("MODEL")
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"ttft_p95_ms":  value,
			"observed_at":  observedAt,
			"sample_count": count,
			"model":        model,
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
