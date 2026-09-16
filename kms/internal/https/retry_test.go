// Copyright 2025 - MinIO, Inc. All rights reserved.
// Use of this source code is governed by the AGPLv3
// license that can be found in the LICENSE file.

package https

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// errDown is returned by probes and round trips for a host that is down.
var errDown = errors.New("host is down")

// roundTripFunc is an http.RoundTripper that reports which hosts it
// was asked for.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestLoadBalancerHostSkipsSuspendedHost(t *testing.T) {
	lb := &LoadBalancer{
		Hosts:         []string{"a", "b", "c"},
		Probe:         func(context.Context, string) error { return errDown },
		ProbeInterval: time.Hour,
	}
	lb.suspend("b")

	for i := 0; i < 100; i++ {
		host, err := lb.Host()
		if err != nil {
			t.Fatalf("Host: %v", err)
		}
		if host == "b" {
			t.Fatal("Host returned the suspended host 'b'")
		}
	}
}

func TestLoadBalancerHostWithAllHostsSuspended(t *testing.T) {
	lb := &LoadBalancer{
		Hosts:         []string{"a", "b", "c"},
		Probe:         func(context.Context, string) error { return errDown },
		ProbeInterval: time.Hour,
	}
	for _, host := range lb.Hosts {
		lb.suspend(host)
	}

	host, err := lb.Host()
	if err != nil {
		t.Fatalf("Host: %v", err)
	}
	if host == "" {
		t.Fatal("Host returned no host while all hosts are suspended")
	}
}

func TestLoadBalancerProbeKeepsUnresponsiveHostSuspended(t *testing.T) {
	lb := &LoadBalancer{
		Hosts:         []string{"a", "b"},
		Timeout:       time.Millisecond,
		Probe:         func(context.Context, string) error { return errDown },
		ProbeInterval: time.Millisecond,
	}
	lb.suspend("a")

	// Well past Timeout, which must not re-admit a host that a Probe
	// decides about.
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 100; i++ {
		if host, _ := lb.Host(); host != "b" {
			t.Fatalf("Host = %q, want the only host that responds", host)
		}
	}
}

func TestLoadBalancerProbeReadmitsHostOnlyOnceItResponds(t *testing.T) {
	live := make(chan struct{})
	probed := make(chan string, 1024)

	lb := &LoadBalancer{
		Hosts:         []string{"a", "b"},
		ProbeInterval: time.Millisecond,
	}
	lb.Probe = func(_ context.Context, host string) error {
		probed <- host
		select {
		case <-live:
			return nil
		default:
			return errDown
		}
	}
	lb.suspend("a")

	// For as long as 'a' does not respond, it must not be re-admitted,
	// however often it is probed.
	for i := 0; i < 5; i++ {
		if host := <-probed; host != "a" {
			t.Fatalf("probed host = %q, want the suspended host 'a'", host)
		}
		if host, _ := lb.Host(); host != "b" {
			t.Fatalf("Host = %q, want the only host that is not suspended", host)
		}
	}

	close(live)
	waitFor(t, "'a' to be re-admitted once it responds", func() bool {
		return !lb.suspended("a")
	})

	// Once no host is suspended anymore, probing must stop so that it
	// does not outlive the outage.
	waitFor(t, "probing to stop", func() bool {
		lb.mu.RLock()
		defer lb.mu.RUnlock()
		return !lb.probing
	})
}

func TestLoadBalancerTimeoutReadmitsHostWithoutProbe(t *testing.T) {
	lb := &LoadBalancer{
		Hosts:   []string{"a", "b"},
		Timeout: 10 * time.Millisecond,
	}
	lb.suspend("a")

	if host, _ := lb.Host(); host != "b" {
		t.Fatalf("Host = %q, want the only host that is not suspended", host)
	}

	waitFor(t, "'a' to be re-admitted after the timeout elapsed", func() bool {
		for i := 0; i < 100; i++ {
			if host, _ := lb.Host(); host == "a" {
				return true
			}
		}
		return false
	})
}

func TestLoadBalancerRoundTripSuspendsFailedHost(t *testing.T) {
	probed := make(chan string, 1024)

	lb := &LoadBalancer{
		Hosts: []string{"a", "b"},
		RoundTripper: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host == "a" {
				return nil, errDown
			}
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}),
		ProbeInterval: time.Millisecond,
	}
	lb.Probe = func(_ context.Context, host string) error {
		probed <- host
		return errDown
	}

	resp, err := lb.RoundTrip(&http.Request{URL: &url.URL{Scheme: "https", Host: "a"}})
	if err != nil {
		t.Fatalf("RoundTrip did not retry the request with a different host: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if !lb.suspended("a") {
		t.Fatal("RoundTrip did not suspend the host its request failed for")
	}
	if host := <-probed; host != "a" {
		t.Fatalf("probed host = %q, want the suspended host 'a'", host)
	}
}

// suspended reports whether host is currently suspended. It exists for
// tests only; production code consults isSuspended while holding lb.mu.
func (lb *LoadBalancer) suspended(host string) bool {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	_, ok := lb.timeout[host]
	return ok
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	const Timeout = 10 * time.Second
	for deadline := time.Now().Add(Timeout); time.Now().Before(deadline); {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", Timeout, what)
}
