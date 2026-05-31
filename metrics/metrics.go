package metrics

import (
	"expvar"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

var (
	mu       sync.Mutex
	counters = map[string]*expvar.Int{}
)

// Inc increments a named pipeline counter.
func Inc(pipeline, name string, delta int64) {
	counter(pipeline, name).Add(delta)
}

// ObserveDuration records a duration as milliseconds in a named counter.
func ObserveDuration(pipeline, name string, d time.Duration) {
	Inc(pipeline, name+"_ms_total", d.Milliseconds())
}

// StartServer starts the built-in expvar endpoint.
func StartServer(addr string, prometheusEnabled ...bool) *http.Server {
	if addr == "" {
		addr = ":9090"
	}
	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	if len(prometheusEnabled) > 0 && prometheusEnabled[0] {
		mux.HandleFunc("/metrics", prometheusHandler)
	}
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		_ = srv.ListenAndServe()
	}()
	return srv
}

func prometheusHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	for _, key := range snapshotKeys() {
		v := expvar.Get(key)
		if v == nil {
			continue
		}
		fmt.Fprintf(w, "# TYPE %s counter\n%s %s\n", key, key, v.String())
	}
}

func snapshotKeys() []string {
	mu.Lock()
	defer mu.Unlock()
	keys := make([]string, 0, len(counters))
	for key := range counters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func counter(pipeline, name string) *expvar.Int {
	key := fmt.Sprintf("go_etl_%s_%s", sanitize(pipeline), sanitize(name))

	mu.Lock()
	defer mu.Unlock()

	if v, ok := counters[key]; ok {
		return v
	}
	v := expvar.NewInt(key)
	counters[key] = v
	return v
}

func sanitize(s string) string {
	if s == "" {
		return "default"
	}
	out := []byte(s)
	for i, b := range out {
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' {
			continue
		}
		out[i] = '_'
	}
	return string(out)
}
