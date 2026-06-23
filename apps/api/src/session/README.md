# Sessions

The sessions product gives organizations warm, pre-provisioned sandboxes for low-latency code
execution (one-shot `code-run` and streaming `connect`), backed by an in-sandbox `session-daemon`.

Core pieces:

- `services/session.service.ts` — request entrypoints (`codeRun`, `connect`, `createSession`,
  transient sessions) and the API-internal daemon client.
- `services/session-repository.service.ts` — context-id → sandbox resolution (Postgres + Redis cache).
- `services/session-pool.service.ts` — the warm-sandbox fleet lifecycle.
- `services/session-scheduler.service.ts` + `services/session-load.service.ts` — instance selection
  and load tracking.
- `services/session-gc.service.ts` — idle/absolute TTL GC for context rows.

## Scale-out

The pool autoscales from one warm sandbox per `(org, template)` to a hybrid-autoscaled fleet that
distributes concurrent load and provisions/reaps sandboxes on demand. See
[docs/scale-out.md](./docs/scale-out.md) for the full design: request routing & stickiness, the load
model, cgroup/PSI methodology, the autoscale algorithm, the config reference, and the ops runbook.
