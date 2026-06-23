// Copyright Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

//go:build e2e && scaleout

// This file is an on-demand, executable spec for session scale-out. It is gated
// behind the extra `scaleout` build tag (on top of `e2e`) so it is NOT compiled
// into the default `go test -tags e2e` runs — i.e. it never runs as part of the
// `e2e` / `e2e:sessions` targets or CI. Run it explicitly:
//
//	DAYTONA_API_URL=http://localhost:3001/api DAYTONA_API_KEY=e2e_admin_api_key \
//	  npx nx run daytona-e2e:e2e:scaleout
//
// As of today it is EXPECTED TO FAIL: the session pool keeps exactly one warm
// sandbox per (org, template) and the in-sandbox daemon multiplexes all
// concurrent operations on it (see SessionPoolService: "exactly one
// SessionInstance per (org, template)"). The assertions below encode the desired
// behavior — under concurrent load the feature should spin up additional
// sandboxes and distribute the load across them — and will pass once scale-out
// is implemented.

package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSessionScaleOut fires N concurrent code-run operations against a single
// session template and asserts that execution is spread across multiple
// sandboxes (scale-out) rather than pinned to one warm sandbox.
//
// Observation is implementation-agnostic: each op prints its in-sandbox
// hostname (== container == sandbox), so the set of distinct hostnames is the
// number of sandboxes that actually served load, and the per-hostname histogram
// is the load distribution. A secondary check corroborates via the admin
// sandbox list filtered by the pool's `daytona.io/session-template` label.
func TestSessionScaleOut(t *testing.T) {
	cfg := LoadConfig(t)
	api := NewAPIClient(cfg)

	template := scaleoutEnvStr("SESSION_SCALEOUT_TEMPLATE", "python-default")
	concurrency := scaleoutEnvInt(t, "SESSION_SCALEOUT_CONCURRENCY", 24)
	minSandboxes := scaleoutEnvInt(t, "SESSION_SCALEOUT_MIN_SANDBOXES", 2)
	holdMs := scaleoutEnvInt(t, "SESSION_SCALEOUT_HOLD_MS", 3000)
	maxShare := scaleoutEnvFloat(t, "SESSION_SCALEOUT_MAX_SHARE", 0.8)

	require.Greater(t, concurrency, 1, "SESSION_SCALEOUT_CONCURRENCY must be > 1")
	require.GreaterOrEqual(t, minSandboxes, 2, "SESSION_SCALEOUT_MIN_SANDBOXES must be >= 2 to be a meaningful scale-out assertion")

	// A long-timeout client: when scale-out provisions a NEW sandbox, the
	// triggering code-run can take far longer than the shared APIClient's 30s
	// timeout (which would otherwise trip — same failure class as TestGitClone).
	client := &http.Client{Timeout: 5 * time.Minute}

	// Best-effort cleanup: remove the session sandboxes this run produced so an
	// on-demand invocation doesn't leak warm sandboxes. The pool's reconcile
	// cron rolls the now-stale SessionInstance rows.
	t.Cleanup(func() {
		for _, item := range scaleoutListSessionSandboxes(t, api, template) {
			if id, ok := item["id"].(string); ok && id != "" {
				api.DeleteSandbox(t, id)
			}
		}
	})

	// 1. Warm-up: provision the initial instance so the burst isn't entirely
	//    serialized behind a cold first-provision. A cold acquire can briefly
	//    500 (the pool marks the instance READY on sandbox STARTED, before the
	//    in-sandbox daemon is reachable) and a stale instance pointing at an
	//    auto-stopped sandbox is rolled on first touch — so retry until a clean
	//    200 baseline exists.
	var warmBody map[string]interface{}
	warmOK := false
	for attempt := 1; attempt <= 6; attempt++ {
		status, body, err := scaleoutCodeRun(client, cfg, "import socket; print(socket.gethostname())")
		warmBody = body
		if err == nil && status == http.StatusOK {
			warmOK = true
			t.Logf("warm-up succeeded on attempt %d; sandbox hostname=%q", attempt, strings.TrimSpace(scaleoutStdout(body)))
			break
		}
		t.Logf("warm-up attempt %d/6: status=%d err=%v (body=%v)", attempt, status, err, body)
		time.Sleep(5 * time.Second)
	}
	require.Truef(t, warmOK, "warm-up code-run never returned 200 (last body=%v) — cannot evaluate scale-out without a working baseline sandbox", warmBody)

	// 2. Burst: release all goroutines simultaneously via a barrier channel.
	holdSec := float64(holdMs) / 1000.0
	code := fmt.Sprintf("import socket, time\ntime.sleep(%.3f)\nprint(socket.gethostname())", holdSec)

	type opResult struct {
		status   int
		hostname string
		body     map[string]interface{}
		err      error
	}
	results := make([]opResult, concurrency)
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-barrier
			status, body, err := scaleoutCodeRun(client, cfg, code)
			results[idx] = opResult{
				status:   status,
				hostname: strings.TrimSpace(scaleoutStdout(body)),
				body:     body,
				err:      err,
			}
		}(i)
	}
	close(barrier)
	wg.Wait()

	// 3. Aggregate. Per-op failures are non-fatal (assert, not require) so the
	//    test continues to the scale-out/distribution assertions and reports all
	//    findings together.
	histogram := map[string]int{}
	success := 0
	for i, r := range results {
		if r.err != nil {
			assert.NoErrorf(t, r.err, "op %d: request error", i)
			continue
		}
		if !assert.Equalf(t, http.StatusOK, r.status, "op %d: non-200 (body=%v)", i, r.body) {
			continue
		}
		if !assert.NotEmptyf(t, r.hostname, "op %d: empty hostname (body=%v)", i, r.body) {
			continue
		}
		success++
		histogram[r.hostname]++
	}

	distinct := len(histogram)
	maxCount := 0
	for _, c := range histogram {
		if c > maxCount {
			maxCount = c
		}
	}
	t.Logf("scale-out result: concurrency=%d success=%d distinctSandboxes=%d histogram=%v", concurrency, success, distinct, histogram)

	// Scale-out: execution must land on multiple sandboxes.
	assert.GreaterOrEqualf(t, distinct, minSandboxes,
		"expected the session feature to spin up >= %d sandboxes under %d concurrent ops, but execution landed on %d distinct sandbox(es) — scale-out/load-distribution not yet implemented",
		minSandboxes, concurrency, distinct)

	// Load distribution: no single sandbox may serve more than maxShare of the load.
	if success > 0 {
		allowed := int(math.Ceil(float64(success) * maxShare))
		assert.LessOrEqualf(t, maxCount, allowed,
			"load not distributed: one sandbox served %d/%d ops (cap=%d at %.0f%% max share)",
			maxCount, success, allowed, maxShare*100)
	}

	// Corroboration: count started sandboxes labeled with this template.
	items := scaleoutListSessionSandboxes(t, api, template)
	started := 0
	for _, item := range items {
		if state, _ := item["state"].(string); state == "started" {
			started++
		}
	}
	t.Logf("session-template=%q sandboxes: total=%d started=%d", template, len(items), started)
	assert.GreaterOrEqualf(t, started, minSandboxes,
		"expected >= %d started sandboxes labeled daytona.io/session-template=%q, found %d", minSandboxes, template, started)

	// Keep the existing no-leak invariant: a code-run response must not expose
	// sandbox identifiers even on the scale-out path.
	for _, r := range results {
		if r.body != nil {
			assertNoSandboxLeak(t, r.body, "")
			break
		}
	}
}

// scaleoutCodeRun POSTs a one-shot code-run using the supplied long-timeout
// client (the shared APIClient's 30s timeout is too short for the provision-on-
// demand path). Returns the HTTP status, parsed body, and any transport error.
func scaleoutCodeRun(client *http.Client, cfg Config, code string) (int, map[string]interface{}, error) {
	payload, err := json.Marshal(map[string]interface{}{"language": "python", "code": code})
	if err != nil {
		return 0, nil, err
	}
	url := strings.TrimRight(cfg.BaseURL, "/") + "/sessions/code-run"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	var parsed map[string]interface{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &parsed)
	}
	return resp.StatusCode, parsed, nil
}

// scaleoutListSessionSandboxes returns every sandbox labeled with the given
// session template via GET /sandbox/paginated. Returns nil (and logs) if the
// list call fails, so corroboration degrades gracefully.
func scaleoutListSessionSandboxes(t *testing.T, api *APIClient, template string) []map[string]interface{} {
	t.Helper()
	path := `/sandbox/paginated?labels={"daytona.io/session-template":"` + template + `"}`
	resp, raw := api.DoRequest(t, http.MethodGet, path, nil)
	if resp.StatusCode != http.StatusOK {
		t.Logf("scaleoutListSessionSandboxes(%q): list returned %d", template, resp.StatusCode)
		return nil
	}
	var page struct {
		Items []map[string]interface{} `json:"items"`
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Logf("scaleoutListSessionSandboxes(%q): cannot parse body: %v", template, err)
		return nil
	}
	return page.Items
}

// scaleoutStdout extracts the `stdout` string from a parsed code-run body.
func scaleoutStdout(body map[string]interface{}) string {
	if body == nil {
		return ""
	}
	s, _ := body["stdout"].(string)
	return s
}

func scaleoutEnvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func scaleoutEnvInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return n
}

func scaleoutEnvFloat(t *testing.T, key string, def float64) float64 {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return f
}
