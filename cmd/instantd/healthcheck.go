package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)

func healthcheck(args []string) error {
	flags := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	endpoint := flags.String("url", "", "health endpoint (default: local daemon /health)")
	tlsEndpoint := flags.String("tls-url", "", "optional HTTPS endpoint to verify outbound system trust")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("healthcheck: unexpected arguments")
	}
	if *endpoint == "" {
		addr := os.Getenv("INSTANT_V2_HTTP_ADDR")
		if addr == "" {
			addr = ":8080"
		}
		if port := os.Getenv("INSTANT_V2_HTTP_PORT"); port != "" {
			addr = net.JoinHostPort("", port)
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("healthcheck: invalid daemon address: %w", err)
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "127.0.0.1"
		}
		*endpoint = "http://" + net.JoinHostPort(host, port) + "/health"
	}
	client := &http.Client{
		Timeout:       5 * time.Second,
		Transport:     &http.Transport{}, // loopback readiness must not depend on proxy settings
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	resp, err := client.Get(*endpoint)
	if err != nil {
		return fmt.Errorf("healthcheck: probe: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var result struct{ OK, DB bool }
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&result); err != nil {
		return fmt.Errorf("healthcheck: invalid response: %w", err)
	}
	if !result.OK || !result.DB {
		return fmt.Errorf("healthcheck: database is not ready")
	}
	if *tlsEndpoint != "" {
		u, err := url.Parse(*tlsEndpoint)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			return fmt.Errorf("healthcheck: tls-url must be an HTTPS endpoint without credentials")
		}
		tlsResp, err := client.Get(u.String())
		if err != nil {
			return fmt.Errorf("healthcheck: outbound TLS: %w", err)
		}
		defer func() { _ = tlsResp.Body.Close() }()
		if tlsResp.TLS == nil || len(tlsResp.TLS.VerifiedChains) == 0 {
			return fmt.Errorf("healthcheck: outbound TLS is not verified")
		}
	}
	return nil
}
