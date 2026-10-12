package conductor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMountRoutes_LivenessOK(t *testing.T) {
	pool := newUnconnectedPool(t)
	defer pool.Close()
	cd, err := NewConductor(newTestConfig(t, pool))
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}

	mux := http.NewServeMux()
	cd.MountRoutes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + LivenessPath)
	if err != nil {
		t.Fatalf("GET %s: %v", LivenessPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", string(body), "ok")
	}
}

func TestMountRoutes_LivenessRejectsNonGET(t *testing.T) {
	pool := newUnconnectedPool(t)
	defer pool.Close()
	cd, err := NewConductor(newTestConfig(t, pool))
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}

	mux := http.NewServeMux()
	cd.MountRoutes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Post(srv.URL+LivenessPath, "application/json", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestMountRoutes_PanicsOnNilMux(t *testing.T) {
	pool := newUnconnectedPool(t)
	defer pool.Close()
	cd, err := NewConductor(newTestConfig(t, pool))
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil mux")
		}
	}()
	cd.MountRoutes(nil)
}
