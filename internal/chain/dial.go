package chain

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// DefaultRPCTimeout is how long one endpoint gets to accept a connection and
// then to start answering a request. The slowest call the worker makes, a
// 5000-block log filter, answers in under a second, so 15s only ever cuts off
// an endpoint that is not going to answer.
const DefaultRPCTimeout = 15 * time.Second

// DialRPC connects to the chain RPC. rpcURL is one endpoint, or several
// separated by commas in order of preference.
//
// go-ethereum's own HTTP client has no timeout, so a route that drops packets
// used to hold a call until the kernel gave up on the connection, and a call
// made with a context that has no deadline (every watcher pass) held the loop
// with it. Here each endpoint gets `timeout` per request, and a request that
// fails on one endpoint is sent to the next. A second endpoint only helps if
// it does not share a host or an upstream provider with the first.
//
// A single ws:// or ipc endpoint is dialled as before, without either.
func DialRPC(rpcURL string, timeout time.Duration) (*ethclient.Client, error) {
	var targets []*url.URL
	for raw := range strings.SplitSeq(rpcURL, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("RPC_URL: %w", err)
		}
		targets = append(targets, u)
	}
	if len(targets) == 0 {
		return nil, errors.New("RPC_URL is empty")
	}
	for _, u := range targets {
		if u.Scheme == "http" || u.Scheme == "https" {
			continue
		}
		if len(targets) > 1 {
			return nil, fmt.Errorf("RPC_URL: only http(s) endpoints can be listed together, got %q", u.Scheme)
		}
		return ethclient.Dial(u.String())
	}

	base := http.DefaultTransport.(*http.Transport).Clone()
	base.DialContext = (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext
	base.TLSHandshakeTimeout = timeout
	base.ResponseHeaderTimeout = timeout
	httpClient := &http.Client{
		Transport: &failoverTransport{base: base, targets: targets},
		// The ceiling for one call: a full wait on every endpoint, then
		// the same again to read the answer.
		Timeout: timeout * time.Duration(len(targets)+1),
	}
	client, err := rpc.DialOptions(context.Background(), targets[0].String(), rpc.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	return ethclient.NewClient(client), nil
}

// failoverTransport sends each request to the endpoint that answered last,
// and moves on to the next one when it does not answer or answers 5xx (what a
// proxy returns for a node that is down).
//
// It stays on the endpoint it moved to and returns to an earlier one only
// when that one fails in turn, or on restart. Endpoints are not compared with
// each other: a request that moves mid-pass can land on a node a block or two
// behind. Add a timed return to the first endpoint if a backup turns out to
// be one the worker should not live on.
type failoverTransport struct {
	base    http.RoundTripper
	targets []*url.URL
	current atomic.Int32
}

func (t *failoverTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := int(t.current.Load())
	var lastErr error
	for i := range t.targets {
		idx := (start + i) % len(t.targets)
		last := i == len(t.targets)-1

		attempt := req.Clone(req.Context())
		attempt.URL = t.targets[idx]
		attempt.Host = t.targets[idx].Host
		if i > 0 {
			// The previous attempt consumed the body.
			if req.GetBody == nil {
				return nil, lastErr
			}
			body, err := req.GetBody()
			if err != nil {
				return nil, err
			}
			attempt.Body = body
		}

		resp, err := t.base.RoundTrip(attempt)
		if err == nil && (resp.StatusCode < 500 || last) {
			if resp.StatusCode < 500 {
				t.current.Store(int32(idx))
			}
			return resp, nil
		}
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			err = fmt.Errorf("%s answered %s", t.targets[idx].Host, resp.Status)
		}
		lastErr = err
		// The caller giving up (a stage budget, shutdown) says nothing
		// about the endpoint.
		if req.Context().Err() != nil {
			return nil, lastErr
		}
		if len(t.targets) > 1 {
			next := (idx + 1) % len(t.targets)
			t.current.CompareAndSwap(int32(idx), int32(next))
			slog.Warn("chain RPC endpoint failed, using the next one",
				"failed", t.targets[idx].Host, "next", t.targets[next].Host, "error", err)
		}
	}
	return nil, lastErr
}
