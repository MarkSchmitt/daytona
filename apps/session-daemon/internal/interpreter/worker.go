// Copyright Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package interpreter

// Worker is the contract every execution backend implements. A Worker owns
// the lifecycle of one logical context's execution slot — a subprocess for
// Python, a V8 session slot inside a shared host process for TypeScript.
//
// The Worker emits chunks via the supplied chunk-handler closure. Implementations
// may use one goroutine per worker (Python) or share one goroutine across many
// contexts (TS host); either way they MUST tag chunks with the right context id
// before invoking the handler.
type Worker interface {
	// Send queues a single command and returns immediately. The chunk handler
	// passed at construction time is invoked from a worker-owned goroutine for
	// each chunk. When the worker emits a {type:"control", text:"completed"|
	// "interrupted"} chunk, the caller knows the command is done.
	Send(cmd WorkerCommand) error

	// Interrupt asks the worker to abort the currently running command.
	// For subprocess workers this typically sends SIGINT then SIGKILL after
	// gracePeriod. For V8 session workers it disposes and recreates the session.
	Interrupt() error

	// Shutdown tears the worker down and returns any teardown error so the
	// caller can log it. After Shutdown returns, Send and Interrupt are no-ops
	// and the worker may be discarded.
	Shutdown() error

	// Active reports whether the worker is currently usable.
	Active() bool
}
