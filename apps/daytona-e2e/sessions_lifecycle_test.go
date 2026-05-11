// Copyright Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

//go:build e2e

package e2e_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSessionCreateRunDelete verifies that two code-run calls reusing the same context
// share state (variable set in call 1 visible in call 2).
func TestSessionCreateRunDelete(t *testing.T) {
	t.Skipf("not yet implemented: session-entity, session-cache")

	cfg := LoadConfig(t)
	api := NewAPIClient(cfg)
	ic := NewSessionClient(api)

	created, status := ic.CreateSession(t, map[string]interface{}{
		"template": "python-default",
		"language": "python",
	})
	require.Equal(t, http.StatusCreated, status, "POST /sessions must return 201")

	id, _ := created["id"].(string)
	require.NotEmpty(t, id, "context id must be present")
	assertNoSandboxLeak(t, created, "")

	t.Cleanup(func() { _ = ic.DeleteSession(t, id) })

	// First exec sets a variable.
	body1, status1 := ic.CodeRun(t, map[string]interface{}{
		"context": map[string]string{"id": id},
		"code":    "x = 42",
	})
	require.Equal(t, http.StatusOK, status1)
	assertNoSandboxLeak(t, body1, "")

	// Second exec reads the variable from the persisted context.
	body2, status2 := ic.CodeRun(t, map[string]interface{}{
		"context": map[string]string{"id": id},
		"code":    "print(x)",
	})
	require.Equal(t, http.StatusOK, status2)
	stdout, _ := body2["stdout"].(string)
	assert.Equal(t, "42\n", stdout, "context state must persist between calls")

	// Delete returns 204.
	require.Equal(t, http.StatusNoContent, ic.DeleteSession(t, id))
}

// TestSessionListNoLeak verifies that listing contexts shows ACTIVE rows with the right
// shape and never leaks sandbox identifiers.
func TestSessionListNoLeak(t *testing.T) {
	t.Skipf("not yet implemented: session-entity, session-gc")

	cfg := LoadConfig(t)
	api := NewAPIClient(cfg)
	ic := NewSessionClient(api)

	created, status := ic.CreateSession(t, map[string]interface{}{
		"template": "python-default",
		"language": "python",
	})
	require.Equal(t, http.StatusCreated, status)
	id, _ := created["id"].(string)
	t.Cleanup(func() { _ = ic.DeleteSession(t, id) })

	contexts, listStatus := ic.ListSessions(t, "")
	require.Equal(t, http.StatusOK, listStatus)
	require.NotEmpty(t, contexts)

	var found map[string]interface{}
	for _, raw := range contexts {
		ctx, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		assertNoSandboxLeak(t, ctx, "")
		if ctxID, _ := ctx["id"].(string); ctxID == id {
			found = ctx
		}
	}
	require.NotNil(t, found, "newly created context must be in list")

	_, hasExpiresAt := found["expiresAt"]
	assert.True(t, hasExpiresAt, "context must expose expiresAt")
}

// TestSessionOmitTemplateOnUse proves that the API resolves both template and language
// from a stored context row when the caller passes only `{context:{id}}`.
func TestSessionOmitTemplateOnUse(t *testing.T) {
	t.Skipf("not yet implemented: session-service-controller")

	cfg := LoadConfig(t)
	api := NewAPIClient(cfg)
	ic := NewSessionClient(api)

	created, status := ic.CreateSession(t, map[string]interface{}{
		"template": "python-default",
		"language": "python",
	})
	require.Equal(t, http.StatusCreated, status)
	id, _ := created["id"].(string)
	t.Cleanup(func() { _ = ic.DeleteSession(t, id) })

	body, runStatus := ic.CodeRun(t, map[string]interface{}{
		"context": map[string]string{"id": id},
		"code":    "print('hello')",
	})
	require.Equal(t, http.StatusOK, runStatus, "code-run with only context (no template/language) must succeed")

	stdout, _ := body["stdout"].(string)
	assert.Equal(t, "hello\n", stdout)
}
