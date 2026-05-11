// Copyright Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

//go:build e2e

package e2e_test

import (
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSessionResponsesHideSandboxIdentifiers walks every JSON field in every session API response
// (templates, contexts, code-run, connect) and asserts:
//  1. NO field name matches the forbidden sandbox-leak pattern (defensive — none are in v1 DTOs).
//  2. The actual SessionInstance.sandboxId UUID for the test org+template never appears as a
//     value anywhere in any response body (catches a leak under a renamed field).
//
// Step (2) requires the e2e DB connection; if not configured, runs only the field-name check.
func TestSessionResponsesHideSandboxIdentifiers(t *testing.T) {
	t.Skipf("not yet implemented: session-service-controller")

	cfg := LoadConfig(t)
	api := NewAPIClient(cfg)
	ic := NewSessionClient(api)

	// Trigger sandbox provisioning so a sandboxId exists to look up.
	_, _ = ic.CodeRun(t, map[string]interface{}{"language": "python", "code": "1"})

	knownSandboxID := lookupSandboxIDForTemplate(t, "python-default")

	// 1. /templates
	templates, _ := ic.ListTemplates(t)
	for _, raw := range templates {
		assertNoSandboxLeak(t, raw, knownSandboxID)
	}

	// 2. /code-run (one-shot)
	body, _ := ic.CodeRun(t, map[string]interface{}{"language": "python", "code": "print(1)"})
	assertNoSandboxLeak(t, body, knownSandboxID)

	// 3. /connect
	conn, _ := ic.Connect(t, map[string]interface{}{
		"template": "python-default", "language": "python",
	})
	if id, _ := conn["sessionId"].(string); id != "" {
		t.Cleanup(func() { _ = ic.DeleteSession(t, id) })
	}
	assertNoSandboxLeak(t, conn, knownSandboxID)

	// 4. /sessions (POST)
	created, status := ic.CreateSession(t, map[string]interface{}{
		"template": "python-default", "language": "python",
	})
	if status == http.StatusCreated {
		id, _ := created["id"].(string)
		t.Cleanup(func() { _ = ic.DeleteSession(t, id) })
		assertNoSandboxLeak(t, created, knownSandboxID)
	}

	// 5. /sessions (GET)
	contexts, _ := ic.ListSessions(t, "")
	for _, ctx := range contexts {
		assertNoSandboxLeak(t, ctx, knownSandboxID)
	}

	// 6. /templates/:name/packages
	pkgs, _ := ic.ListPackages(t, "python-default", "python")
	for _, p := range pkgs {
		assertNoSandboxLeak(t, p, knownSandboxID)
	}
}

// lookupSandboxIDForTemplate stub mirrors lookupSandboxIDForContext. Until the test is
// unskipped, it returns "" so that step (2) of the leak check is effectively a no-op.
func lookupSandboxIDForTemplate(t *testing.T, _ string) string {
	t.Helper()
	if os.Getenv("DAYTONA_E2E_DB_URL") == "" {
		return ""
	}
	require.Fail(t, "lookupSandboxIDForTemplate not implemented yet; set DAYTONA_E2E_DB_URL only after wiring this helper")
	return ""
}

// lookupEnv is a thin wrapper so test files can avoid importing os in many places.
func lookupEnv(key string) string {
	return os.Getenv(key)
}
