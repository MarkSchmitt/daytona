# --- Build stage ----------------------------------------------------------
FROM node:22-alpine AS build

ENV CI=true

RUN npm install -g corepack && corepack enable

COPY --from=golang:1.25.4-alpine /usr/local/go/ /usr/local/go/

ENV PATH="/usr/local/go/bin:${PATH}"

RUN apk add --no-cache git

WORKDIR /daytona

# Yarn caching layer
COPY package.json yarn.lock .yarnrc.yml ./
RUN yarn install --immutable

# Nx config
COPY nx.json ./

# Go dependency layer (cached unless go.mod/go.sum change)
COPY go.work go.work.sum ./
COPY apps/session-daemon/go.mod apps/session-daemon/go.sum apps/session-daemon/
RUN head -1 go.work > go.work.tmp && printf '\nuse (\n\t./apps/session-daemon\n)\n' >> go.work.tmp && mv go.work.tmp go.work

ENV NX_DAEMON=false
ENV GONOSUMDB=github.com/daytonaio/daytona

RUN cd apps/session-daemon && go mod download

# Go source
COPY apps/session-daemon/ apps/session-daemon/

ARG VERSION=0.0.1
RUN --mount=type=cache,target=/root/.cache/go-build \
  VERSION=$VERSION yarn nx build session-daemon --configuration=production --nxBail=true

# --- Runtime stage --------------------------------------------------------
# Final image is the curated session runtime: Python venv + Node 22 + curated
# node_modules + the daytona-session-daemon binary baked at /opt/daytona/.
# This is the image that backs the python-default template snapshot.
FROM debian:bookworm-slim AS session-daemon

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV NODE_ENV=production
ENV SESSION_DAEMON_PORT=2281
ENV SESSION_DAEMON_BIND_ADDR=127.0.0.1
ENV SESSION_DAEMON_USER_NODE_MODULES_ROOT=/workspace
ENV SESSION_DAEMON_NODE_BUNDLE_ROOT=/usr/lib/daytona/repl_host
ENV PATH="/opt/daytona/venv/bin:${PATH}"

# OS deps + Python + Node.js 22
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl python3 python3-venv python3-pip build-essential \
      libffi-dev libssl-dev pkg-config \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Curated Python venv. Pinning here is intentional — see plan §1: snapshot is
# the only install path, runtime install is not supported in v1. Bumping these
# requires rebuilding the image (and republishing the python-default snapshot).
RUN python3 -m venv /opt/daytona/venv && \
    /opt/daytona/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/daytona/venv/bin/pip install --no-cache-dir \
      numpy pandas matplotlib requests httpx pydantic openai anthropic

# Curated user-side node_modules. Pure-JS only (no native bindings) — V8 session
# constraint, see plan §1.
RUN mkdir -p /workspace && cd /workspace \
    && npm init -y >/dev/null \
    && npm install --omit=optional --omit=peer \
        zod lodash-es date-fns papaparse marked openai @anthropic-ai/sdk \
    && rm -rf /root/.npm

# Host-side bundle dependencies (isolated-vm, esbuild-wasm). isolated-vm has
# native bindings compiled against this image's node version at install time;
# baking it into the image (rather than runtime install) is what makes
# cold-start fast. Per plan §1, the daemon's //go:embed only carries the entry
# script — heavy deps live in the image layer.
RUN mkdir -p /usr/lib/daytona/repl_host && cd /usr/lib/daytona/repl_host \
    && npm init -y >/dev/null \
    && npm install --omit=optional --omit=peer isolated-vm@5 esbuild-wasm@0.24 \
    && rm -rf /root/.npm

COPY --from=build /daytona/dist/apps/session-daemon /opt/daytona/session-daemon
RUN chmod +x /opt/daytona/session-daemon

# Healthcheck targets the loopback port the daemon binds to. Because the daemon
# is loopback-only, only an in-container healthcheck makes sense.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${SESSION_DAEMON_PORT}/healthz || exit 1

# The runner spawns daytona-daemon as the entrypoint and uses the snapshot's
# `entrypoint` field as the daemon's session command. The seed migration sets
# entrypoint=["/opt/daytona/session-daemon"], so the daemon launches us as the
# child process. This Dockerfile's ENTRYPOINT/CMD is a fallback for direct
# `docker run` usage.
ENTRYPOINT ["/opt/daytona/session-daemon"]
