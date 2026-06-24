/*
 * Copyright Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { SessionGcService } from './session-gc.service'
import { sessionKeys } from './session-repository.service'
import { SessionState } from '../enums/session-state.enum'
import { TypedConfigService } from '../../config/typed-config.service'
import { FakeRedis } from './test-utils/fake-redis'

const ORG = 'org-1'
const INST = 'inst-1'

function makeConfig(overrides: Record<string, number> = {}): TypedConfigService {
  const map: Record<string, number> = {
    'session.context.gcBatchSize': 500,
    'session.context.expiredGracePeriodSeconds': 60,
    ...overrides,
  }
  return { get: (key: string) => map[key] } as unknown as TypedConfigService
}

function seedContext(redis: FakeRedis, id: string, state: SessionState, extra: Record<string, unknown> = {}): void {
  redis.strings.set(
    sessionKeys.ctx(id),
    JSON.stringify({
      id,
      orgId: ORG,
      instanceId: INST,
      language: 'python',
      state,
      createdAt: new Date().toISOString(),
      lastUsedAt: new Date().toISOString(),
      ...extra,
    }),
  )
}

describe('SessionGcService', () => {
  describe('sweepExpired', () => {
    it('flips due ACTIVE contexts to EXPIRED and moves them to the grace zset', async () => {
      const redis = new FakeRedis()
      const gc = new SessionGcService(redis as any, makeConfig())
      const past = Date.now() - 1000

      seedContext(redis, 'c1', SessionState.ACTIVE)
      await redis.zadd(sessionKeys.gcExpiry, past, 'c1')
      await redis.zadd(sessionKeys.orgIndex(ORG), Date.now(), 'c1')

      await gc.sweepExpired()

      const raw = await redis.get(sessionKeys.ctx('c1'))
      const blob = JSON.parse(raw ?? '{}')
      expect(blob.state).toBe(SessionState.EXPIRED)
      expect(blob.expiredAt).toBeDefined()
      // Dropped from active/expiry indexes, queued for hard delete.
      expect(await redis.zrangebyscore(sessionKeys.gcExpiry, '-inf', '+inf')).toHaveLength(0)
      expect(await redis.zrevrange(sessionKeys.orgIndex(ORG), 0, -1)).toHaveLength(0)
      expect(await redis.zrangebyscore(sessionKeys.gcGrace, '-inf', '+inf')).toEqual(['c1'])
    })

    it('leaves contexts whose expiry is still in the future untouched', async () => {
      const redis = new FakeRedis()
      const gc = new SessionGcService(redis as any, makeConfig())

      seedContext(redis, 'c1', SessionState.ACTIVE)
      await redis.zadd(sessionKeys.gcExpiry, Date.now() + 60_000, 'c1')

      await gc.sweepExpired()

      const raw = await redis.get(sessionKeys.ctx('c1'))
      const blob = JSON.parse(raw ?? '{}')
      expect(blob.state).toBe(SessionState.ACTIVE)
      expect(await redis.zrangebyscore(sessionKeys.gcGrace, '-inf', '+inf')).toHaveLength(0)
    })
  })

  describe('hardDeleteExpired', () => {
    it('removes contexts past the grace deadline and cleans their indexes', async () => {
      const redis = new FakeRedis()
      const gc = new SessionGcService(redis as any, makeConfig())
      const past = Date.now() - 1000

      seedContext(redis, 'c1', SessionState.EXPIRED, { expiredAt: new Date(past).toISOString() })
      await redis.zadd(sessionKeys.gcGrace, past, 'c1')
      await redis.sadd(sessionKeys.instanceContexts(INST), 'c1')

      await gc.hardDeleteExpired()

      expect(await redis.get(sessionKeys.ctx('c1'))).toBeNull()
      expect(await redis.zrangebyscore(sessionKeys.gcGrace, '-inf', '+inf')).toHaveLength(0)
      expect(await redis.smembers(sessionKeys.instanceContexts(INST))).toHaveLength(0)
    })

    it('leaves contexts still within the grace window', async () => {
      const redis = new FakeRedis()
      const gc = new SessionGcService(redis as any, makeConfig())

      seedContext(redis, 'c1', SessionState.EXPIRED)
      await redis.zadd(sessionKeys.gcGrace, Date.now() + 60_000, 'c1')

      await gc.hardDeleteExpired()

      expect(await redis.get(sessionKeys.ctx('c1'))).not.toBeNull()
    })
  })
})
