package metrics

import (
	"expvar"
	"fmt"
	"net/http"
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
func StartServer(addr string) *http.Server {
	if addr == "" {
		addr = ":9090"
	}
	srv := &http.Server{Addr: addr}
	go func() {
		_ = srv.ListenAndServe()
	}()
	return srv
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
