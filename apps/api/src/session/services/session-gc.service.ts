/*
 * Copyright Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { Injectable, Logger } from '@nestjs/common'
import { Cron, CronExpression } from '@nestjs/schedule'
import { InjectRedis } from '@nestjs-modules/ioredis'
import Redis from 'ioredis'
import { SessionState } from '../enums/session-state.enum'
import { TypedConfigService } from '../../config/typed-config.service'
import { sessionKeys } from './session-repository.service'

/** Minimal view of a stored context blob the GC needs to clean up secondary indexes. */
interface GcContext {
  orgId: string
  instanceId: string
  state: SessionState
}

/**
 * SessionGcService enforces idle and absolute TTLs on Redis-backed contexts.
 *
 *  - sweepExpired() flips ACTIVE contexts whose expiry deadline has passed to EXPIRED. Candidates
 *    come from the `session:gc:expiry` zset (scored by the computed expiresAt, refreshed on every
 *    lastUsedAt touch), so a single ZRANGEBYSCORE up to `now` yields exactly the due contexts.
 *  - hardDeleteExpired() permanently removes EXPIRED/INVALID contexts past the grace period, taken
 *    from the `session:gc:grace` zset (scored by the grace deadline).
 *
 * Both crons run @EVERY_MINUTE; the grace zset preserves the 410 (expired/invalidated, with
 * reason) contract during the grace window. TTL knobs are re-read from process.env on every tick.
 */
@Injectable()
export class SessionGcService {
  private readonly logger = new Logger(SessionGcService.name)

  constructor(
    @InjectRedis()
    private readonly redis: Redis,
    private readonly config: TypedConfigService,
  ) {}

  @Cron(CronExpression.EVERY_MINUTE, { name: 'session-gc-sweep' })
  async sweepExpired(): Promise<void> {
    const batch = this.config.get('session.context.gcBatchSize') ?? 500
    const now = Date.now()
    const graceSec = this.intEnv(
      'SESSION_EXPIRED_GRACE_SECONDS',
      this.config.get('session.context.expiredGracePeriodSeconds') ?? 86400,
    )
    const graceDeadline = now + graceSec * 1000

    let ids: string[]
    try {
      ids = await this.redis.zrangebyscore(sessionKeys.gcExpiry, '-inf', now, 'LIMIT', 0, batch)
    } catch (err) {
      this.logger.error(`sweepExpired candidate scan failed: ${err.message}`)
      return
    }
    if (ids.length === 0) return

    let raws: (string | null)[]
    try {
      raws = await this.redis.mget(ids.map((id) => sessionKeys.ctx(id)))
    } catch (err) {
      this.logger.error(`sweepExpired blob read failed: ${err.message}`)
      return
    }

    const pipe = this.redis.pipeline()
    let expired = 0
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i]
      const raw = raws[i]
      // Blob gone or no longer ACTIVE — just drop the stale expiry-index entry.
      if (!raw) {
        pipe.zrem(sessionKeys.gcExpiry, id)
        continue
      }
      const blob = JSON.parse(raw) as GcContext & Record<string, unknown>
      if (blob.state !== SessionState.ACTIVE) {
        pipe.zrem(sessionKeys.gcExpiry, id)
        continue
      }
      blob.state = SessionState.EXPIRED
      blob.expiredAt = new Date(now).toISOString()
      pipe.set(sessionKeys.ctx(id), JSON.stringify(blob))
      pipe.zrem(sessionKeys.orgIndex(blob.orgId), id)
      pipe.zrem(sessionKeys.gcExpiry, id)
      pipe.zadd(sessionKeys.gcGrace, graceDeadline, id)
      expired++
    }

    try {
      await pipe.exec()
    } catch (err) {
      this.logger.error(`sweepExpired pipeline failed: ${err.message}`)
      return
    }
    if (expired > 0) this.logger.debug(`sweepExpired: marked ${expired} contexts EXPIRED`)
  }

  @Cron(CronExpression.EVERY_MINUTE, { name: 'session-gc-hard-delete' })
  async hardDeleteExpired(): Promise<void> {
    const batch = this.config.get('session.context.gcBatchSize') ?? 500
    const now = Date.now()

    let ids: string[]
    try {
      ids = await this.redis.zrangebyscore(sessionKeys.gcGrace, '-inf', now, 'LIMIT', 0, batch)
    } catch (err) {
      this.logger.error(`hardDeleteExpired candidate scan failed: ${err.message}`)
      return
    }
    if (ids.length === 0) return

    let raws: (string | null)[]
    try {
      raws = await this.redis.mget(ids.map((id) => sessionKeys.ctx(id)))
    } catch (err) {
      this.logger.error(`hardDeleteExpired blob read failed: ${err.message}`)
      return
    }

    const pipe = this.redis.pipeline()
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i]
      const raw = raws[i]
      pipe.del(sessionKeys.ctx(id))
      pipe.zrem(sessionKeys.gcGrace, id)
      if (raw) {
        const blob = JSON.parse(raw) as GcContext
        pipe.srem(sessionKeys.instanceContexts(blob.instanceId), id)
        pipe.zrem(sessionKeys.orgIndex(blob.orgId), id)
      }
    }

    try {
      await pipe.exec()
      this.logger.debug(`hardDeleteExpired: removed ${ids.length} contexts`)
    } catch (err) {
      this.logger.error(`hardDeleteExpired pipeline failed: ${err.message}`)
    }
  }

  private intEnv(name: string, fallback: number): number {
    const raw = process.env[name]
    if (!raw) return fallback
    const n = parseInt(raw, 10)
    return Number.isFinite(n) && n >= 0 ? n : fallback
  }
}
