/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { Injectable, Logger } from '@nestjs/common'
import { Cron, CronExpression } from '@nestjs/schedule'
import { InjectRepository } from '@nestjs/typeorm'
import { Repository } from 'typeorm'
import { InjectRedis } from '@nestjs-modules/ioredis'
import Redis from 'ioredis'
import { Session } from '../entities/session.entity'
import { SessionState } from '../enums/session-state.enum'
import { TypedConfigService } from '../../config/typed-config.service'

/**
 * SessionGcService enforces idle and absolute TTLs on contexts.
 *
 * Both crons are hardcoded at @Cron(EVERY_MINUTE):
 *  - sweepExpired() marks ACTIVE contexts that have idled out / absolutely aged out as EXPIRED.
 *  - hardDeleteExpired() permanently removes EXPIRED/INVALID rows older than the grace period.
 *
 * Hard-delete runs every minute (not hourly) so e2e tests with overridden grace periods can
 * complete in seconds rather than waiting an hour. Steady-state cost is one indexed DELETE
 * LIMIT 500 per minute on a small table — negligible.
 *
 * TTL knobs are re-read from process.env on every tick (not at boot) so tests can flip them
 * without an API restart.
 */
@Injectable()
export class SessionGcService {
  private readonly logger = new Logger(SessionGcService.name)

  constructor(
    @InjectRepository(Session)
    private readonly repo: Repository<Session>,
    @InjectRedis()
    private readonly redis: Redis,
    private readonly config: TypedConfigService,
  ) {}

  @Cron(CronExpression.EVERY_MINUTE, { name: 'session-gc-sweep' })
  async sweepExpired(): Promise<void> {
    const idleTtlSec = this.intEnv(
      'SESSION_IDLE_TTL_SECONDS',
      this.config.get('session.context.idleTtlSeconds') ?? 3600,
    )
    const absTtlSec = this.intEnv(
      'SESSION_ABSOLUTE_TTL_SECONDS',
      this.config.get('session.context.absoluteTtlSeconds') ?? 604800,
    )
    const batch = this.config.get('session.context.gcBatchSize') ?? 500

    let updatedRows: { id: string; instanceId: string }[] = []
    try {
      const result = await this.repo
        .createQueryBuilder()
        .update(Session)
        .set({ state: SessionState.EXPIRED, expiredAt: () => 'NOW()' })
        .where(
          `id IN (
             SELECT id FROM session
             WHERE state = :active
               AND ("lastUsedAt" < NOW() - (:idle * INTERVAL '1 second')
                    OR "createdAt" < NOW() - (:abs * INTERVAL '1 second'))
             LIMIT :batch
           )`,
          {
            active: SessionState.ACTIVE,
            idle: idleTtlSec,
            abs: absTtlSec,
            batch,
          },
        )
        .returning(['id', 'instanceId'])
        .execute()
      updatedRows = (result.raw as { id: string; instanceId: string }[] | undefined) ?? []
    } catch (err) {
      this.logger.error(`sweepExpired query failed: ${err.message}`)
      return
    }

    if (updatedRows.length === 0) return
    this.logger.debug(`sweepExpired: marked ${updatedRows.length} contexts EXPIRED`)

    // Best-effort Redis cleanup. Daemon-side DELETE /sessions is fired by the SessionService
    // path on next access — we don't tunnel it from here to keep the GC PG-only.
    try {
      const pipe = this.redis.pipeline()
      for (const row of updatedRows) {
        pipe.del(`session:${row.id}`)
        pipe.srem(`session:instance:${row.instanceId}:sessions`, row.id)
      }
      await pipe.exec()
    } catch (err) {
      this.logger.warn(`sweepExpired cache cleanup failed: ${err.message}`)
    }
  }

  @Cron(CronExpression.EVERY_MINUTE, { name: 'session-gc-hard-delete' })
  async hardDeleteExpired(): Promise<void> {
    const graceSec = this.intEnv(
      'SESSION_EXPIRED_GRACE_SECONDS',
      this.config.get('session.context.expiredGracePeriodSeconds') ?? 86400,
    )
    const batch = this.config.get('session.context.gcBatchSize') ?? 500
    const cutoff = new Date(Date.now() - graceSec * 1000)

    try {
      const expired = await this.repo
        .createQueryBuilder()
        .delete()
        .from(Session)
        .where(
          `id IN (
             SELECT id FROM session
             WHERE (state = :expired AND "expiredAt" < :cutoff)
                OR (state = :invalid AND "invalidatedAt" < :cutoff)
             LIMIT :batch
           )`,
          {
            expired: SessionState.EXPIRED,
            invalid: SessionState.INVALID,
            cutoff,
            batch,
          },
        )
        .execute()
      if (expired.affected) {
        this.logger.debug(`hardDeleteExpired: removed ${expired.affected} rows`)
      }
    } catch (err) {
      this.logger.error(`hardDeleteExpired failed: ${err.message}`)
    }
  }

  private intEnv(name: string, fallback: number): number {
    const raw = process.env[name]
    if (!raw) return fallback
    const n = parseInt(raw, 10)
    return Number.isFinite(n) ? n : fallback
  }
}
