package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCountersIncrement(t *testing.T) {
	before := testutil.ToFloat64(FanoutBytes)
	FanoutBytes.Add(1234)
	if got := testutil.ToFloat64(FanoutBytes); got != before+1234 {
		t.Fatalf("FanoutBytes = %v, want %v", got, before+1234)
	}

	rBefore := testutil.ToFloat64(Refreshes.WithLabelValues("spliced"))
	Refreshes.WithLabelValues("spliced").Inc()
	if got := testutil.ToFloat64(Refreshes.WithLabelValues("spliced")); got != rBefore+1 {
		t.Fatalf("Refreshes{spliced} = %v, want %v", got, rBefore+1)
	}

	sBefore := testutil.ToFloat64(NotifierSheds)
	NotifierSheds.Inc()
	if got := testutil.ToFloat64(NotifierSheds); got != sBefore+1 {
		t.Fatalf("NotifierSheds = %v, want %v", got, sBefore+1)
	}
}

func TestRegisterGaugeServesScrapeTimeValue(t *testing.T) {
	RegisterGauge("instant_test_scrape_gauge", "test gauge", nil, nil, func() float64 { return 42 })

	body := scrapeRegistry(t)
	if !strings.Contains(body, "instant_test_scrape_gauge 42") {
		t.Fatalf("scrape output missing live gauge value:\n%s", body)
	}
}

func TestGaugeRegistrationLifecycle(t *testing.T) {
	var oldCalls, newCalls atomic.Int64
	old := RegisterGauge("instant_test_lifecycle_gauge", "test gauge", nil, nil, func() float64 {
		oldCalls.Add(1)
		return 1
	})
	if body := scrapeRegistry(t); !strings.Contains(body, "instant_test_lifecycle_gauge 1") {
		t.Fatalf("initial gauge missing from scrape:\n%s", body)
	}

	current := RegisterGauge("instant_test_lifecycle_gauge", "test gauge", nil, nil, func() float64 {
		newCalls.Add(1)
		return 2
	})
	body := scrapeRegistry(t)
	if got := metricSampleCount(body, "instant_test_lifecycle_gauge"); got != 1 {
		t.Fatalf("replacement emitted %d samples, want 1:\n%s", got, body)
	}
	if !strings.Contains(body, "instant_test_lifecycle_gauge 2") {
		t.Fatalf("replacement gauge missing from scrape:\n%s", body)
	}
	if oldCalls.Load() != 1 {
		t.Fatalf("replaced callback called %d times, want 1", oldCalls.Load())
	}
	if newCalls.Load() != 1 {
		t.Fatalf("replacement callback called %d times, want 1", newCalls.Load())
	}

	old.Close()
	body = scrapeRegistry(t)
	if !strings.Contains(body, "instant_test_lifecycle_gauge 2") {
		t.Fatalf("closing replaced owner removed active gauge:\n%s", body)
	}
	current.Close()
	current.Close()
	if body = scrapeRegistry(t); metricSampleCount(body, "instant_test_lifecycle_gauge") != 0 {
		t.Fatalf("closed gauge remained in scrape:\n%s", body)
	}

	left := RegisterGauge("instant_test_lifecycle_labeled", "test gauge", []string{"owner"}, []string{"left"}, func() float64 { return 3 })
	right := RegisterGauge("instant_test_lifecycle_labeled", "test gauge", []string{"owner"}, []string{"right"}, func() float64 { return 4 })
	body = scrapeRegistry(t)
	if got := metricSampleCount(body, "instant_test_lifecycle_labeled"); got != 2 {
		t.Fatalf("independent owners emitted %d samples, want 2:\n%s", got, body)
	}
	left.Close()
	body = scrapeRegistry(t)
	if got := metricSampleCount(body, "instant_test_lifecycle_labeled"); got != 1 || !strings.Contains(body, `instant_test_lifecycle_labeled{owner="right"} 4`) {
		t.Fatalf("closing one owner affected the other (%d samples):\n%s", got, body)
	}
	right.Close()
}

func metricSampleCount(body, name string) int {
	count := 0
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			count++
		}
	}
	return count
}

func TestGaugeRegistrationConcurrentScrapeLifecycle(t *testing.T) {
	const name = "instant_test_concurrent_gauge"
	active := RegisterGauge(name, "test gauge", nil, nil, func() float64 { return 7 })

	var wg sync.WaitGroup
	errs := make(chan int, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 64; i++ {
			w := httptest.NewRecorder()
			Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
			if w.Code != 200 {
				select {
				case errs <- w.Code:
				default:
				}
				return
			}
		}
	}()
	for i := 0; i < 64; i++ {
		registration := RegisterGauge(name, "test gauge", nil, nil, func() float64 { return 7 })
		registration.Close()
	}
	wg.Wait()
	select {
	case code := <-errs:
		t.Fatalf("concurrent metrics scrape status = %d, want 200", code)
	default:
	}
	active.Close()
}

func TestGaugeCloseReleasesCallbackStorage(t *testing.T) {
	var collector fnCollector
	registration := collector.add("owned", nil, nil, func() float64 { return 1 })
	registration.Close()
	if len(collector.entries) != 0 {
		t.Fatal("closed callback remains registered")
	}
	if collector.entries[:cap(collector.entries)][0].value != nil {
		t.Fatal("closed callback retained in backing storage")
	}
}

func TestHandlerExposesCoreSeries(t *testing.T) {
	// CounterVec/HistogramVec children materialize on first use; touch the
	// label combinations production code uses so the scrape exposes them.
	// ws_sessions_active is callback-registered by cmd wiring; mirror that.
	RegisterGauge("instant_ws_sessions_active",
		"Live websocket/SSE sessions.", nil, nil, func() float64 { return 0 })
	RefreshFrames.WithLabelValues("full")
	RefreshFrames.WithLabelValues("delta")
	TransactDuration.WithLabelValues("runtime")
	TransactDuration.WithLabelValues("admin")
	TransactDuration.WithLabelValues("ws")
	RateLimitRejections.WithLabelValues("transact")

	body := scrapeRegistry(t)
	for _, want := range []string{
		"instant_ws_sessions_active",
		"instant_ws_refresh_frames_total",
		"instant_fanout_bytes_total",
		"instant_refreshes_total",
		"instant_notifier_sheds_total",
		"instant_transact_duration_seconds",
		"instant_ratelimit_rejections_total",
		"instant_bus_publish_errors_total",
		"instant_bus_events_received_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("registry missing series %s", want)
		}
	}
}
