/*
 * Copyright Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { SessionInstanceStore } from './session-instance-store.service'
import { SessionInstanceState } from '../enums/session-instance-state.enum'
import { SessionInstanceRole } from '../enums/session-instance-role.enum'
import { FakeRedis } from './test-utils/fake-redis'

const ORG = 'org-1'
const TPL = 'tpl-1'

function newStore(): { store: SessionInstanceStore; redis: FakeRedis } {
  const redis = new FakeRedis()
  return { store: new SessionInstanceStore(redis as any), redis }
}

describe('SessionInstanceStore', () => {
  it('creates an instance discoverable by id, state, and org/template/state', async () => {
    const { store } = newStore()
    const inst = await store.create({ organizationId: ORG, templateId: TPL, role: SessionInstanceRole.WARM })

    expect(inst.id).toBeDefined()
    expect(inst.state).toBe(SessionInstanceState.PROVISIONING)
    expect(await store.findById(inst.id)).toMatchObject({ id: inst.id, organizationId: ORG })
    expect((await store.findByState(SessionInstanceState.PROVISIONING)).map((i) => i.id)).toEqual([inst.id])
    expect((await store.findByOrgTemplateState(ORG, TPL, SessionInstanceState.PROVISIONING)).map((i) => i.id)).toEqual([
      inst.id,
    ])
    expect(await store.countByState(ORG, TPL, SessionInstanceState.PROVISIONING)).toBe(1)
  })

  it('re-indexes state membership on a state transition', async () => {
    const { store } = newStore()
    const inst = await store.create({ organizationId: ORG, templateId: TPL })

    inst.state = SessionInstanceState.READY
    await store.save(inst)

    // Gone from the PROVISIONING indexes, present in the READY ones.
    expect(await store.findByState(SessionInstanceState.PROVISIONING)).toHaveLength(0)
    expect(await store.countByState(ORG, TPL, SessionInstanceState.PROVISIONING)).toBe(0)
    expect((await store.findByState(SessionInstanceState.READY)).map((i) => i.id)).toEqual([inst.id])
    expect(await store.countByState(ORG, TPL, SessionInstanceState.READY)).toBe(1)
  })

  it('update() patches fields without changing index membership', async () => {
    const { store } = newStore()
    const inst = await store.create({ organizationId: ORG, templateId: TPL, state: SessionInstanceState.READY })

    const when = new Date()
    await store.update(inst.id, { lastActiveAt: when })

    const reloaded = await store.findById(inst.id)
    expect(reloaded?.lastActiveAt?.getTime()).toBe(when.getTime())
    expect(await store.countByState(ORG, TPL, SessionInstanceState.READY)).toBe(1)
  })

  it('delete() removes the blob and all index entries', async () => {
    const { store } = newStore()
    const inst = await store.create({ organizationId: ORG, templateId: TPL, state: SessionInstanceState.READY })

    await store.delete(inst.id)

    expect(await store.findById(inst.id)).toBeNull()
    expect(await store.findByState(SessionInstanceState.READY)).toHaveLength(0)
    expect(await store.countByState(ORG, TPL, SessionInstanceState.READY)).toBe(0)
  })

  it('prunes a dangling index id whose blob has disappeared', async () => {
    const { store, redis } = newStore()
    // Simulate a crashed writer that left an id in the state index but no blob.
    await redis.sadd('session:inst:state:ready', 'ghost')

    expect(await store.findByState(SessionInstanceState.READY)).toHaveLength(0)
    // The dangling member was pruned as a side effect.
    expect(await redis.scard('session:inst:state:ready')).toBe(0)
  })
})
