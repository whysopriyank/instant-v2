package metrics

import (
	"strings"
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
