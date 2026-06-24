/*
 * Copyright Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { Injectable, Logger } from '@nestjs/common'
import { InjectRedis } from '@nestjs-modules/ioredis'
import Redis from 'ioredis'
import { v4 as uuidv4 } from 'uuid'
import { SessionInstance } from '../entities/session-instance.entity'
import { SessionInstanceState } from '../enums/session-instance-state.enum'
import { SessionInstanceRole } from '../enums/session-instance-role.enum'

/**
 * Redis persistence for SessionInstance — the warm-sandbox fleet's pool state. Replaces the
 * former Postgres table.
 *
 * Data model:
 *  - `session:inst:{id}`                                  JSON blob (source of truth).
 *  - `session:inst:state:{state}`                         SET of instance ids (global by state).
 *  - `session:inst:org:{orgId}:tpl:{templateId}:state:{s}`SET of instance ids (per-fleet by state).
 *
 * Every mutation re-derives index membership from the previous blob, so the state sets stay
 * consistent. There is intentionally NO TTL on instance keys: a warm instance must persist for as
 * long as its sandbox is alive. Redis may still be wiped (treated as ephemeral) — the pool's
 * orphan-sandbox reconciler is what prevents a wipe from leaking the underlying sandboxes.
 */
@Injectable()
export class SessionInstanceStore {
  private readonly logger = new Logger(SessionInstanceStore.name)

  constructor(
    @InjectRedis()
    private readonly redis: Redis,
  ) {}

  private static KEY(id: string): string {
    return `session:inst:${id}`
  }

  private static STATE_IDX(state: SessionInstanceState): string {
    return `session:inst:state:${state}`
  }

  private static ORG_TPL_STATE_IDX(orgId: string, templateId: string, state: SessionInstanceState): string {
    return `session:inst:org:${orgId}:tpl:${templateId}:state:${state}`
  }

  async create(input: {
    organizationId: string
    templateId: string
    snapshotId?: string
    sandboxId?: string
    state?: SessionInstanceState
    role?: SessionInstanceRole
  }): Promise<SessionInstance> {
    const now = new Date()
    const inst = new SessionInstance()
    inst.id = uuidv4()
    inst.organizationId = input.organizationId
    inst.templateId = input.templateId
    inst.snapshotId = input.snapshotId as string
    inst.sandboxId = input.sandboxId
    inst.state = input.state ?? SessionInstanceState.PROVISIONING
    inst.role = input.role ?? SessionInstanceRole.WARM
    inst.createdAt = now
    inst.updatedAt = now
    await this.write(inst, null)
    return inst
  }

  /** Upsert an instance, re-indexing state membership against the persisted previous blob. */
  async save(inst: SessionInstance): Promise<SessionInstance> {
    const prev = await this.findById(inst.id)
    inst.updatedAt = new Date()
    await this.write(inst, prev)
    return inst
  }

  /** Best-effort partial update (e.g. lastActiveAt) that leaves state/index membership intact. */
  async update(id: string, patch: Partial<SessionInstance>): Promise<void> {
    const prev = await this.findById(id)
    if (!prev) return
    Object.assign(prev, patch)
    prev.updatedAt = new Date()
    await this.write(prev, prev)
  }

  async findById(id: string): Promise<SessionInstance | null> {
    const raw = await this.redis.get(SessionInstanceStore.KEY(id))
    return raw ? this.deserialize(raw) : null
  }

  async findByOrgTemplateState(
    orgId: string,
    templateId: string,
    state: SessionInstanceState,
  ): Promise<SessionInstance[]> {
    const ids = await this.redis.smembers(SessionInstanceStore.ORG_TPL_STATE_IDX(orgId, templateId, state))
    return this.mget(ids)
  }

  async countByState(orgId: string, templateId: string, state: SessionInstanceState): Promise<number> {
    // Count via the live blobs (not SCARD) so a stale id left behind by a crashed writer doesn't
    // inflate the cap-enforcement count; mget prunes any dangling index members as a side effect.
    return (await this.findByOrgTemplateState(orgId, templateId, state)).length
  }

  async findByState(state: SessionInstanceState): Promise<SessionInstance[]> {
    const ids = await this.redis.smembers(SessionInstanceStore.STATE_IDX(state))
    return this.mget(ids)
  }

  async delete(id: string): Promise<void> {
    const prev = await this.findById(id)
    const pipe = this.redis.pipeline()
    pipe.del(SessionInstanceStore.KEY(id))
    if (prev) {
      pipe.srem(SessionInstanceStore.STATE_IDX(prev.state), id)
      pipe.srem(SessionInstanceStore.ORG_TPL_STATE_IDX(prev.organizationId, prev.templateId, prev.state), id)
    }
    await pipe.exec()
  }

  // -- internals ------------------------------------------------------------

  private async write(inst: SessionInstance, prev: SessionInstance | null): Promise<void> {
    const pipe = this.redis.pipeline()
    // Drop stale index membership when the state (or, defensively, org/template) changed.
    if (prev) {
      const stateChanged = prev.state !== inst.state
      const fleetChanged = prev.organizationId !== inst.organizationId || prev.templateId !== inst.templateId
      if (stateChanged || fleetChanged) {
        pipe.srem(SessionInstanceStore.STATE_IDX(prev.state), inst.id)
        pipe.srem(SessionInstanceStore.ORG_TPL_STATE_IDX(prev.organizationId, prev.templateId, prev.state), inst.id)
      }
    }
    pipe.set(SessionInstanceStore.KEY(inst.id), this.serialize(inst))
    pipe.sadd(SessionInstanceStore.STATE_IDX(inst.state), inst.id)
    pipe.sadd(SessionInstanceStore.ORG_TPL_STATE_IDX(inst.organizationId, inst.templateId, inst.state), inst.id)
    await pipe.exec()
  }

  /** Fetch many instances, skipping (and pruning) ids whose blob has disappeared. */
  private async mget(ids: string[]): Promise<SessionInstance[]> {
    if (ids.length === 0) return []
    const raws = await this.redis.mget(ids.map((id) => SessionInstanceStore.KEY(id)))
    const out: SessionInstance[] = []
    const dangling: string[] = []
    raws.forEach((raw, i) => {
      if (raw) out.push(this.deserialize(raw))
      else dangling.push(ids[i])
    })
    if (dangling.length > 0) {
      this.pruneDangling(dangling).catch((err) => this.logger.debug(`prune dangling index ids failed: ${err.message}`))
    }
    return out
  }

  /** Remove ids that point at a missing blob from every state index they could live in. */
  private async pruneDangling(ids: string[]): Promise<void> {
    const pipe = this.redis.pipeline()
    for (const id of ids) {
      for (const state of Object.values(SessionInstanceState)) {
        pipe.srem(SessionInstanceStore.STATE_IDX(state), id)
      }
    }
    await pipe.exec()
  }

  private serialize(inst: SessionInstance): string {
    return JSON.stringify({
      id: inst.id,
      organizationId: inst.organizationId,
      templateId: inst.templateId,
      snapshotId: inst.snapshotId,
      sandboxId: inst.sandboxId,
      state: inst.state,
      errorReason: inst.errorReason,
      role: inst.role,
      lastUsedAt: inst.lastUsedAt?.toISOString(),
      lastActiveAt: inst.lastActiveAt?.toISOString(),
      createdAt: inst.createdAt.toISOString(),
      updatedAt: inst.updatedAt.toISOString(),
    })
  }

  private deserialize(raw: string): SessionInstance {
    const o = JSON.parse(raw)
    const inst = new SessionInstance()
    inst.id = o.id
    inst.organizationId = o.organizationId
    inst.templateId = o.templateId
    inst.snapshotId = o.snapshotId
    inst.sandboxId = o.sandboxId ?? undefined
    inst.state = o.state
    inst.errorReason = o.errorReason ?? undefined
    inst.role = o.role ?? SessionInstanceRole.WARM
    inst.lastUsedAt = o.lastUsedAt ? new Date(o.lastUsedAt) : undefined
    inst.lastActiveAt = o.lastActiveAt ? new Date(o.lastActiveAt) : undefined
    inst.createdAt = new Date(o.createdAt)
    inst.updatedAt = new Date(o.updatedAt)
    return inst
  }
}
