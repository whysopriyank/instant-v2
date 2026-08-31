// Package metrics owns instantd's Prometheus collectors (docs/reference/09-tier2-
// architecture.md §T3 observability). Collectors are package-level so hot
// paths pay an atomic add and nothing else; gauges that mirror live state
// (notifier queue depth, pool saturation, session count) are registered as
// scrape-time callbacks so no polling goroutine exists.
//
// Everything hangs off Registry (not prometheus.DefaultRegisterer) so the
// /metrics endpoint serves exactly instantd's series plus the standard Go /
// process collectors, never third-party noise pulled in transitively.
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds every instantd series. Served verbatim by Handler.
var Registry = prometheus.NewRegistry()

// Handler returns the HTTP handler for the /metrics endpoint.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}

func init() {
	Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

// RefreshFrames counts delivered refresh waves, split by frame kind. The
// delta kind only fires for sessions that negotiated delta-refresh; its
// ratio against full is the wire-savings proof for T2's incremental work.
var RefreshFrames = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "instant",
	Name:      "ws_refresh_frames_total",
	Help:      "Refresh frames fanned out, by frame kind.",
}, []string{"kind"})

// FanoutBytes counts payload bytes handed to a transport after a successful
// send — the denominator for compression savings and the fanout rate the
// audit asks for.
var FanoutBytes = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "instant",
	Name:      "fanout_bytes_total",
	Help:      "Refresh payload bytes successfully written to clients.",
})

// Refreshes classifies notifier drain outcomes: spliced (incremental
// maintenance produced the envelope), full (recompute oracle ran), error.
var Refreshes = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "instant",
	Name:      "refreshes_total",
	Help:      "Subscription refresh drains by outcome.",
}, []string{"outcome"})

// NotifierSheds counts transacts denied by the queue-depth gate.
var NotifierSheds = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "instant",
	Name:      "notifier_sheds_total",
	Help:      "Transacts shed by the notifier backpressure gate.",
})

// TransactDuration observes end-to-end transact handling per plane
// (runtime HTTP/WS, admin API): parse -> resolve -> commit -> notify.
var TransactDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "instant",
	Name:      "transact_duration_seconds",
	Help:      "End-to-end transact handling latency by plane.",
	Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
}, []string{"plane"})

// RateLimitRejections counts token-bucket denials per traffic class.
var RateLimitRejections = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "instant",
	Name:      "ratelimit_rejections_total",
	Help:      "Token-bucket denials per traffic class.",
}, []string{"class"})

// BusPublishErrors counts failed LISTEN/NOTIFY publishes.
var BusPublishErrors = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "instant",
	Name:      "bus_publish_errors_total",
	Help:      "Failed invalidation bus publishes.",
})

// BusEventsReceived counts valid invalidations applied from the bus.
var BusEventsReceived = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "instant",
	Name:      "bus_events_received_total",
	Help:      "Valid invalidations received from peers via the bus.",
})

// BusMalformed counts payloads that failed decode on receive.
var BusMalformed = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "instant",
	Name:      "bus_malformed_total",
	Help:      "Bus payloads that failed JSON decoding.",
})

func init() {
	Registry.MustRegister(RefreshFrames, FanoutBytes, Refreshes,
		NotifierSheds, TransactDuration, RateLimitRejections,
		BusPublishErrors, BusEventsReceived, BusMalformed)
}

// fnCollector emits gauge series computed at scrape time — no polling
// goroutine, no staleness: every scrape reads the live value. One instance
// carries every callback-registered series; RegisterGauge appends to it.
type fnCollector struct {
	mu     sync.Mutex
	descs  []*prometheus.Desc
	values []func() float64
	labels [][]string // const label values per desc
}

func (c *fnCollector) add(desc *prometheus.Desc, labelValues []string, fn func() float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.descs = append(c.descs, desc)
	c.values = append(c.values, fn)
	c.labels = append(c.labels, labelValues)
}

func (c *fnCollector) Describe(ch chan<- *prometheus.Desc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, d := range c.descs {
		ch <- d
	}
}

func (c *fnCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, d := range c.descs {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, c.values[i](), c.labels[i]...)
	}
}

var scrapeFns fnCollector

func init() { Registry.MustRegister(&scrapeFns) }

// RegisterGauge exposes a scrape-time gauge. labelValues are the const
// values of the variable labels in labelNames; pass nils for an unlabeled
// series. Duplicate fully-qualified names panic at registration — wiring
// bugs surface at boot, not under load.
func RegisterGauge(name, help string, labelNames, labelValues []string, fn func() float64) {
	scrapeFns.add(prometheus.NewDesc(name, help, labelNames, nil), labelValues, fn)
}
