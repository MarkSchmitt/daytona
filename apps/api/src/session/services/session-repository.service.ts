/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { Injectable, Logger, NotFoundException } from '@nestjs/common'
import { InjectRepository } from '@nestjs/typeorm'
import { Repository } from 'typeorm'
import { InjectRedis } from '@nestjs-modules/ioredis'
import Redis from 'ioredis'
import { v4 as uuidv4 } from 'uuid'
import { Session } from '../entities/session.entity'
import { SessionInstance } from '../entities/session-instance.entity'
import { SessionState } from '../enums/session-state.enum'
import { SessionInstanceState } from '../enums/session-instance-state.enum'
import { SessionDto } from '../dto/session.dto'
import { SessionExpiredError, SessionInvalidatedError } from '../errors/session-errors'
import { TypedConfigService } from '../../config/typed-config.service'

interface CachedContext {
  orgId: string
  instanceId: string
  language: string
  cwd?: string
  state: SessionState
  invalidatedAt?: string
  expiredAt?: string
  createdAt: string
  lastUsedAt: string
  version: number
}

interface CachedInstance {
  id: string
  orgId: string
  templateId: string
  snapshotId: string
  sandboxId?: string
  state: SessionInstanceState
  version: number
}

export interface ResolvedContext {
  context: Session
  instance: SessionInstance
}

const KEY_CONTEXT = (id: string) => `session:${id}`
const KEY_INSTANCE = (id: string) => `session:instance:${id}`
const KEY_INSTANCE_CONTEXTS = (id: string) => `session:instance:${id}:sessions`

/**
 * SessionRepository is the **only** point that resolves a context-id to an in-sandbox
 * target. Postgres is the durable source of truth; Redis is a strict cache layered in front.
 *
 * Cache semantics:
 *  - Read path: single MGET on (context, instance). Any Redis error is logged and treated as
 *    a cache miss, falling through to Postgres.
 *  - Write paths: Postgres-first; Redis pipeline is best-effort.
 *  - In-process request coalescing on cold cache: N concurrent reads for the same context-id
 *    fan in to one Postgres query.
 *  - lastUsedAt is bumped on a fire-and-forget basis, throttled to ≥ 5s per context, since the
 *    column drives autostop heuristics and TTL renewal — not exact accounting.
 */
@Injectable()
export class SessionRepository {
  private readonly logger = new Logger(SessionRepository.name)
  private readonly inFlight = new Map<string, Promise<ResolvedContext>>()
  private readonly lastUsedTouch = new Map<string, number>()

  constructor(
    @InjectRepository(Session)
    private readonly contextRepo: Repository<Session>,
    @InjectRepository(SessionInstance)
    private readonly instanceRepo: Repository<SessionInstance>,
    @InjectRedis()
    private readonly redis: Redis,
    private readonly config: TypedConfigService,
  ) {}

  /** Computes expiresAt = min(lastUsedAt + idleTtl, createdAt + absoluteTtl). */
  computeExpiresAt(createdAt: Date | string, lastUsedAt: Date | string): Date {
    const created = typeof createdAt === 'string' ? new Date(createdAt) : createdAt
    const lastUsed = typeof lastUsedAt === 'string' ? new Date(lastUsedAt) : lastUsedAt
    // Read TTLs from process.env at every call so e2e tests can flip them without restart.
    const idleTtlSec = this.readIntEnv(
      'SESSION_IDLE_TTL_SECONDS',
      this.config.get('session.context.idleTtlSeconds') ?? 3600,
    )
    const absTtlSec = this.readIntEnv(
      'SESSION_ABSOLUTE_TTL_SECONDS',
      this.config.get('session.context.absoluteTtlSeconds') ?? 604800,
    )
    const idleExp = new Date(lastUsed.getTime() + idleTtlSec * 1000)
    const absExp = new Date(created.getTime() + absTtlSec * 1000)
    return idleExp < absExp ? idleExp : absExp
  }

  toDto(c: Session): SessionDto {
    return {
      id: c.id,
      language: c.language,
      cwd: c.cwd,
      createdAt: c.createdAt.toISOString(),
      lastUsedAt: c.lastUsedAt?.toISOString(),
      expiresAt: this.computeExpiresAt(c.createdAt, c.lastUsedAt ?? c.createdAt).toISOString(),
    }
  }

  async create(orgId: string, instance: SessionInstance, opts: { language: string; cwd?: string }): Promise<Session> {
    const id = uuidv4()
    const now = new Date()
    const row = this.contextRepo.create({
      id,
      organizationId: orgId,
      instanceId: instance.id,
      language: opts.language,
      cwd: opts.cwd,
      state: SessionState.ACTIVE,
      createdAt: now,
      lastUsedAt: now,
    })
    const saved = await this.contextRepo.save(row)
    await this.cacheWriteThrough(saved, instance)
    return saved
  }

  async resolve(orgId: string, sessionId: string): Promise<ResolvedContext> {
    const inFlightKey = `${orgId}:${sessionId}`
    const existing = this.inFlight.get(inFlightKey)
    if (existing) return existing

    const promise = this.resolveInner(orgId, sessionId).finally(() => {
      this.inFlight.delete(inFlightKey)
    })
    this.inFlight.set(inFlightKey, promise)
    return promise
  }

  private async resolveInner(orgId: string, sessionId: string): Promise<ResolvedContext> {
    const cached = await this.cacheRead(sessionId)
    if (cached) {
      const { context: ctx, instance: inst } = cached
      this.assertOrgOwnership(ctx, orgId, sessionId)
      this.assertContextActive(ctx, sessionId)
      this.assertInstanceReady(ctx, inst, sessionId)
      // Cache hit: build entity-shaped objects so the call site uses one type.
      const cEntity = this.cachedToContextEntity(ctx, sessionId)
      const iEntity = this.cachedToInstanceEntity(inst)
      this.touchLastUsed(sessionId).catch((err) => this.logger.debug(`touchLastUsed: ${err.message}`))
      return { context: cEntity, instance: iEntity }
    }

    // Postgres fallback (and only durable correctness path).
    const row = await this.contextRepo
      .createQueryBuilder('c')
      .innerJoinAndSelect('c.instance', 'i')
      .where('c.id = :sessionId AND c.organizationId = :orgId', { sessionId, orgId })
      .getOne()

    if (!row || !row.instance) {
      throw new NotFoundException(`Session ${sessionId} not found.`)
    }
    if (row.state === SessionState.INVALID) {
      throw new SessionInvalidatedError(row.id, row.invalidatedAt ?? new Date())
    }
    if (row.state === SessionState.EXPIRED) {
      const reason = this.classifyExpiryReason(row)
      throw new SessionExpiredError(row.id, row.expiredAt ?? new Date(), reason)
    }
    if (row.instance.state !== SessionInstanceState.READY) {
      throw new SessionInvalidatedError(row.id, row.instance.updatedAt ?? new Date())
    }

    await this.cacheWriteThrough(row, row.instance)
    this.touchLastUsed(sessionId).catch((err) => this.logger.debug(`touchLastUsed: ${err.message}`))
    return { context: row, instance: row.instance }
  }

  async delete(orgId: string, sessionId: string): Promise<void> {
    const row = await this.contextRepo.findOne({
      where: { id: sessionId, organizationId: orgId },
    })
    if (!row) return // idempotent
    await this.contextRepo.delete({ id: row.id })
    try {
      await this.redis.pipeline().del(KEY_CONTEXT(row.id)).srem(KEY_INSTANCE_CONTEXTS(row.instanceId), row.id).exec()
    } catch (err) {
      this.logger.warn(`redis cache cleanup on delete failed: ${err.message}`)
    }
  }

  async list(orgId: string, templateId?: string): Promise<SessionDto[]> {
    const qb = this.contextRepo
      .createQueryBuilder('c')
      .innerJoin('c.instance', 'i')
      .where('c.organizationId = :orgId AND c.state = :state', { orgId, state: SessionState.ACTIVE })
      .orderBy('c.createdAt', 'DESC')
    if (templateId) {
      qb.andWhere('i.templateId = :templateId', { templateId })
    }
    const rows = await qb.getMany()
    return rows.map((r) => this.toDto(r))
  }

  /**
   * Bulk-marks all ACTIVE contexts for an instance as INVALID. Called by the pool
   * reconciler when a sandbox dies or a snapshot drift is detected.
   */
  async markInstanceSessionsInvalid(instanceId: string): Promise<void> {
    await this.contextRepo
      .createQueryBuilder()
      .update(Session)
      .set({ state: SessionState.INVALID, invalidatedAt: () => 'NOW()' })
      .where('"instanceId" = :id AND state = :active', { id: instanceId, active: SessionState.ACTIVE })
      .execute()

    try {
      const ids = await this.redis.smembers(KEY_INSTANCE_CONTEXTS(instanceId))
      const pipe = this.redis.pipeline()
      for (const id of ids) pipe.del(KEY_CONTEXT(id))
      pipe.del(KEY_INSTANCE_CONTEXTS(instanceId))
      pipe.del(KEY_INSTANCE(instanceId))
      await pipe.exec()
    } catch (err) {
      this.logger.warn(`bulk-invalidation cache cleanup failed: ${err.message}`)
    }
  }

  // -- internals ------------------------------------------------------------

  private async cacheRead(sessionId: string): Promise<{ context: CachedContext; instance: CachedInstance } | null> {
    try {
      const ctxRaw = await this.redis.get(KEY_CONTEXT(sessionId))
      if (!ctxRaw) return null
      const ctx = JSON.parse(ctxRaw) as CachedContext
      const instRaw = await this.redis.get(KEY_INSTANCE(ctx.instanceId))
      if (!instRaw) return null
      const inst = JSON.parse(instRaw) as CachedInstance
      return { context: ctx, instance: inst }
    } catch (err) {
      this.logger.warn(`session cache read fell through: ${err.message}`)
      return null
    }
  }

  private async cacheWriteThrough(ctx: Session, inst: SessionInstance): Promise<void> {
    const ctxTtl = this.config.get('session.cache.contextTtlSeconds') ?? 300
    const instTtl = this.config.get('session.cache.instanceTtlSeconds') ?? 60
    const ctxBlob: CachedContext = {
      orgId: ctx.organizationId,
      instanceId: ctx.instanceId,
      language: ctx.language,
      cwd: ctx.cwd,
      state: ctx.state,
      invalidatedAt: ctx.invalidatedAt?.toISOString(),
      expiredAt: ctx.expiredAt?.toISOString(),
      createdAt: ctx.createdAt.toISOString(),
      lastUsedAt: ctx.lastUsedAt.toISOString(),
      version: ctx.lastUsedAt.getTime(),
    }
    const instBlob: CachedInstance = {
      id: inst.id,
      orgId: inst.organizationId,
      templateId: inst.templateId,
      snapshotId: inst.snapshotId,
      sandboxId: inst.sandboxId,
      state: inst.state,
      version: (inst.updatedAt ?? new Date()).getTime(),
    }
    try {
      await this.redis
        .pipeline()
        .set(KEY_CONTEXT(ctx.id), JSON.stringify(ctxBlob), 'EX', ctxTtl)
        .set(KEY_INSTANCE(inst.id), JSON.stringify(instBlob), 'EX', instTtl)
        .sadd(KEY_INSTANCE_CONTEXTS(inst.id), ctx.id)
        .exec()
    } catch (err) {
      this.logger.warn(`session cache write-through skipped: ${err.message}`)
    }
  }

  private cachedToContextEntity(c: CachedContext, id: string): Session {
    const e = new Session()
    e.id = id
    e.organizationId = c.orgId
    e.instanceId = c.instanceId
    e.language = c.language
    e.cwd = c.cwd
    e.state = c.state
    e.invalidatedAt = c.invalidatedAt ? new Date(c.invalidatedAt) : undefined
    e.expiredAt = c.expiredAt ? new Date(c.expiredAt) : undefined
    e.createdAt = new Date(c.createdAt)
    e.lastUsedAt = new Date(c.lastUsedAt)
    return e
  }

  private cachedToInstanceEntity(c: CachedInstance): SessionInstance {
    const e = new SessionInstance()
    e.id = c.id
    e.organizationId = c.orgId
    e.templateId = c.templateId
    e.snapshotId = c.snapshotId
    e.sandboxId = c.sandboxId
    e.state = c.state
    return e
  }

  private async touchLastUsed(sessionId: string): Promise<void> {
    const throttleMs = this.config.get('session.cache.lastUsedAtThrottleMs') ?? 5000
    const last = this.lastUsedTouch.get(sessionId) ?? 0
    const now = Date.now()
    if (now - last < throttleMs) return
    this.lastUsedTouch.set(sessionId, now)
    try {
      await this.contextRepo
        .createQueryBuilder()
        .update(Session)
        .set({ lastUsedAt: () => 'NOW()' })
        .where('id = :id AND state = :active', { id: sessionId, active: SessionState.ACTIVE })
        .execute()
    } catch (err) {
      this.logger.debug(`touchLastUsed: ${err.message}`)
    }
  }

  private assertOrgOwnership(c: CachedContext, orgId: string, sessionId: string): void {
    if (c.orgId !== orgId) {
      // Don't leak existence: surface 404, not 403.
      throw new NotFoundException(`Session ${sessionId} not found.`)
    }
  }

  private assertContextActive(c: CachedContext, sessionId: string): void {
    if (c.state === SessionState.INVALID) {
      throw new SessionInvalidatedError(sessionId, c.invalidatedAt ?? new Date().toISOString())
    }
    if (c.state === SessionState.EXPIRED) {
      const reason = this.classifyCachedExpiry(c)
      throw new SessionExpiredError(sessionId, c.expiredAt ?? new Date().toISOString(), reason)
    }
  }

  private assertInstanceReady(c: CachedContext, i: CachedInstance, sessionId: string): void {
    if (i.state !== SessionInstanceState.READY) {
      throw new SessionInvalidatedError(sessionId, new Date().toISOString())
    }
  }

  private classifyExpiryReason(c: Session): 'idle' | 'absolute' {
    const idleTtlSec = this.readIntEnv(
      'SESSION_IDLE_TTL_SECONDS',
      this.config.get('session.context.idleTtlSeconds') ?? 3600,
    )
    const absoluteEdge =
      c.createdAt.getTime() + (this.config.get('session.context.absoluteTtlSeconds') ?? 604800) * 1000
    const idleEdge = c.lastUsedAt.getTime() + idleTtlSec * 1000
    return absoluteEdge <= idleEdge ? 'absolute' : 'idle'
  }

  private classifyCachedExpiry(c: CachedContext): 'idle' | 'absolute' {
    const created = new Date(c.createdAt).getTime()
    const used = new Date(c.lastUsedAt).getTime()
    const idleTtl = this.readIntEnv(
      'SESSION_IDLE_TTL_SECONDS',
      this.config.get('session.context.idleTtlSeconds') ?? 3600,
    )
    const absTtl = this.readIntEnv(
      'SESSION_ABSOLUTE_TTL_SECONDS',
      this.config.get('session.context.absoluteTtlSeconds') ?? 604800,
    )
    return created + absTtl * 1000 <= used + idleTtl * 1000 ? 'absolute' : 'idle'
  }

  private readIntEnv(name: string, fallback: number): number {
    const raw = process.env[name]
    if (!raw) return fallback
    const n = parseInt(raw, 10)
    return Number.isFinite(n) ? n : fallback
  }
}
