package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPCheckpointClient_Contract(t *testing.T) {
	var gotMethod, gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotQuery = r.Method, r.URL.RawQuery
		if r.URL.Path != "/checkpoint" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":42,"checkpoint_id":42,"step":1200,"world_size":3}`))
	}))
	defer server.Close()
	status, err := NewHTTPCheckpointClient().Request(context.Background(), server.URL, 42)
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost || gotQuery != "id=42" || status.CheckpointID != 42 || status.Step != 1200 {
		t.Fatalf("unexpected exchange: %s %s -> %+v", gotMethod, gotQuery, status)
	}
}

func TestHTTPCheckpointClient_RejectsUnexpectedResponses(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"server_error": func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", http.StatusInternalServerError) },
		"bad_json": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(handler)
			defer server.Close()
			if _, err := NewHTTPCheckpointClient().Request(context.Background(), server.URL, 1); err == nil {
				t.Fatal("want an error")
			}
		})
	}
	if _, err := NewHTTPCheckpointClient().Request(context.Background(), "http://127.0.0.1:1", 1); err == nil {
		t.Fatal("want an error for an unreachable trainer")
	}
}
