package main

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/metrics"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

func (a *appRuntime) registerGauges(ws *syncpkg.WSHandler, sseH *syncpkg.SSEHandler) func() {
	// Scrape-time gauges over live state (no polling goroutines).
	registrations := make([]*metrics.GaugeRegistration, 0, 8)
	registrations = append(registrations, metrics.RegisterGauge("instant_notifier_queue_depth",
		"Pending refreshes awaiting a notifier drain.", nil, nil,
		func() float64 { return float64(a.notifier.QueueDepth()) }))
	registrations = append(registrations, metrics.RegisterGauge("instant_ws_sessions_active",
		"Live websocket/SSE sessions.", nil, nil,
		func() float64 { return float64(ws.ConnCount()) + float64(sseH.ConnCount()) }))
	for name, p := range map[string]*pgxpool.Pool{"write": a.pool, "read": a.readPool} {
		for state, fn := range map[string]func() int32{
			"acquired": func() int32 { return p.Stat().AcquiredConns() },
			"idle":     func() int32 { return p.Stat().IdleConns() },
			"max":      func() int32 { return p.Stat().MaxConns() },
		} {
			registrations = append(registrations, metrics.RegisterGauge("instant_db_pool_conns",
				"pgxpool connections by state.", []string{"pool", "state"},
				[]string{name, state}, func() float64 { return float64(fn()) }))
		}
	}
	return func() {
		for _, registration := range registrations {
			registration.Close()
		}
	}
}
