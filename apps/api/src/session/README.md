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

## Bash (isolated) executions

A feasibility/design investigation into running isolated bash executions — where the hard part is
isolating concurrent contexts that share one scale-out sandbox. See
[docs/bash-isolation.md](./docs/bash-isolation.md) for the threat model (intra-org blast-radius vs.
the privileged-container boundary), where a bash worker plugs into the daemon, the isolation options
(process group, cgroups, namespaces, overlay, dedicated sandbox, virtual interpreter), a survey of
existing solutions (`just-bash`, bashkit, E2B, Modal, microsandbox), and a tiered recommendation.
