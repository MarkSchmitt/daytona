/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { randomUUID } from 'crypto'
import { Injectable, Logger, NotFoundException } from '@nestjs/common'
import { Cron, CronExpression } from '@nestjs/schedule'
import { InjectRepository } from '@nestjs/typeorm'
import { LessThan, Repository } from 'typeorm'
import { SessionInstance } from '../entities/session-instance.entity'
import { SessionTemplate } from '../entities/session-template.entity'
import { SessionInstanceState } from '../enums/session-instance-state.enum'
import { SessionInstanceRole } from '../enums/session-instance-role.enum'
import { SandboxService } from '../../sandbox/services/sandbox.service'
import { Organization } from '../../organization/entities/organization.entity'
import { LockCode, RedisLockProvider } from '../../sandbox/common/redis-lock.provider'
import { TypedConfigService } from '../../config/typed-config.service'
import { SessionRepository } from './session-repository.service'
import { SessionLoadService } from './session-load.service'
import { SessionScheduler } from './session-scheduler.service'
import { SandboxState } from '../../sandbox/enums/sandbox-state.enum'
import { SandboxDesiredState } from '../../sandbox/enums/sandbox-desired-state.enum'
import { Sandbox } from '../../sandbox/entities/sandbox.entity'

const POOL_LOCK_TTL_SEC = 120

/**
 * SessionPoolService owns the warm-sandbox fleet per (organizationId, templateId).
 *
 * Scale-out model (see apps/api/src/session/docs/scale-out.md):
 *  - There can be MANY instances per (org, template): a `warm` floor (`minWarm`) plus
 *    autoscaled `overflow` instances, up to `maxInstancesPerTemplate`.
 *  - `acquire` claims a slot (via SessionScheduler) on the least-loaded instance with headroom;
 *    when all are saturated and under cap it provisions a new sandbox; callers MUST release the
 *    claimed slot via SessionLoadService.decrInflight when done.
 *  - When a sandbox dies or its snapshot drifts, the instance is rolled and its Session rows are
 *    atomically marked INVALID (dangling contexts can never be silently re-routed).
 *  - reconcile (30s) rolls dead instances, tops up to `minWarm`, scales in idle `overflow`
 *    instances above `minWarm`, and prunes long-dead ERROR rows.
 */
@Injectable()
export class SessionPoolService {
  private readonly logger = new Logger(SessionPoolService.name)

  constructor(
    @InjectRepository(SessionInstance)
    private readonly instanceRepo: Repository<SessionInstance>,
    @InjectRepository(SessionTemplate)
    private readonly templateRepo: Repository<SessionTemplate>,
    @InjectRepository(Sandbox)
    private readonly sandboxRepo: Repository<Sandbox>,
    private readonly sandboxService: SandboxService,
    private readonly sessions: SessionRepository,
    private readonly lockProvider: RedisLockProvider,
    private readonly config: TypedConfigService,
    private readonly load: SessionLoadService,
    private readonly scheduler: SessionScheduler,
  ) {}

  /**
   * Acquire a usable instance for (org, template) and atomically claim one in-flight slot on it.
   * The caller owns releasing that slot (SessionLoadService.decrInflight) once its op completes.
   */
  async acquire(orgId: string, organization: Organization, template: SessionTemplate): Promise<SessionInstance> {
    const max = this.config.get('session.scale.maxInstancesPerTemplate') ?? 5
    const deadline = Date.now() + (this.config.get('session.provisionTimeoutMs') ?? 180000)

    while (Date.now() < deadline) {
      const live = await this.listLiveReadyInstances(orgId, template)

      // 1. Claim a slot on an existing instance with headroom.
      const claimed = await this.scheduler.claim(live)
      if (claimed) {
        await this.markActive(claimed)
        return claimed
      }

      const provisioning = await this.countByState(orgId, template.id, SessionInstanceState.PROVISIONING)
      const total = live.length + provisioning

      // 2. Add capacity when bootstrapping (nothing exists) or scaling out (ready but saturated).
      const wantNew = (live.length === 0 && provisioning === 0) || live.length > 0
      if (wantNew && total < max) {
        const reserved = await this.tryReserveInstance(orgId, template.id)
        if (reserved) {
          const ready = await this.provisionReserved(reserved, organization, template)
          await this.load.incrInflight(ready.id)
          await this.markActive(ready)
          return ready
        }
        // Lost the reservation race or now at cap — fall through.
      }

      // 3. Capacity is coming online (someone is provisioning) — wait and retry to claim it.
      if (provisioning > 0 || live.length === 0) {
        await this.sleep(1500)
        continue
      }

      // 4. At cap, everything ready and saturated — overload the least-loaded instance.
      const forced = await this.scheduler.claimForce(live)
      if (forced) {
        await this.markActive(forced)
        return forced
      }
      await this.sleep(1000)
    }

    throw new NotFoundException('Timed out acquiring a session instance (fleet busy or at capacity).')
  }

  /**
   * READY instances for (org, template), with dead/drifted ones rolled out before returning.
   */
  private async listLiveReadyInstances(orgId: string, template: SessionTemplate): Promise<SessionInstance[]> {
    const ready = await this.instanceRepo.find({
      where: { organizationId: orgId, templateId: template.id, state: SessionInstanceState.READY },
    })
    const live: SessionInstance[] = []
    for (const inst of ready) {
      if (inst.snapshotId !== template.snapshotId) {
        await this.rollInstance(inst, 'snapshot drift detected at acquire-time')
        continue
      }
      if (!(await this.isSandboxLive(inst))) {
        await this.rollInstance(inst, `sandbox ${inst.sandboxId} no longer started at acquire-time`)
        continue
      }
      live.push(inst)
    }
    return live
  }

  private async countByState(orgId: string, templateId: string, state: SessionInstanceState): Promise<number> {
    return this.instanceRepo.count({ where: { organizationId: orgId, templateId, state } })
  }

  /**
   * Reserve a new instance row under a short per-(org,template) lock: the lock guards the
   * count + insert (fast), and the PROVISIONING row is *persisted while the lock is held* so the
   * next lock holder counts it before this one releases — concurrent scale-ups can each reserve a
   * distinct slot up to the cap, but can never collectively exceed it. The slow sandbox
   * provisioning then happens lock-free in provisionReserved.
   */
  private async tryReserveInstance(orgId: string, templateId: string): Promise<SessionInstance | null> {
    const max = this.config.get('session.scale.maxInstancesPerTemplate') ?? 5
    const minWarm = this.config.get('session.scale.minWarm') ?? 1
    const lockKey = `session:scale:${orgId}:${templateId}`
    // Ownership-aware lock: a unique token per acquisition so a lock that expired (TTL) and was
    // re-acquired by another scale-up is never deleted by this caller's unlock.
    const lockCode = new LockCode(randomUUID())
    if (!(await this.lockProvider.lock(lockKey, POOL_LOCK_TTL_SEC, lockCode))) return null
    try {
      const active =
        (await this.countByState(orgId, templateId, SessionInstanceState.READY)) +
        (await this.countByState(orgId, templateId, SessionInstanceState.PROVISIONING))
      if (active >= max) return null
      const role = active < minWarm ? SessionInstanceRole.WARM : SessionInstanceRole.OVERFLOW
      const row = this.instanceRepo.create({
        organizationId: orgId,
        templateId,
        // snapshotId is set in provisionReserved (needs the template) — placeholder until then.
        state: SessionInstanceState.PROVISIONING,
        role,
      })
      // Persist before releasing the lock so the PROVISIONING row is counted by the next holder.
      return await this.instanceRepo.save(row)
    } finally {
      await this.lockProvider.unlock(lockKey, lockCode).catch(() => undefined)
    }
  }

  /**
   * Provision the sandbox for a reserved (already-persisted PROVISIONING) instance row and wait
   * until it is READY. Updates the row in place (snapshotId + sandboxId); on failure marks it ERROR
   * so reconcile's pruneErroredInstances reaps it.
   */
  private async provisionReserved(
    reserved: SessionInstance,
    organization: Organization,
    template: SessionTemplate,
  ): Promise<SessionInstance> {
    reserved.snapshotId = template.snapshotId
    reserved.state = SessionInstanceState.PROVISIONING
    let saved = await this.instanceRepo.save(reserved)
    try {
      const idleTtlSec = this.config.get('session.context.idleTtlSeconds') ?? 3600
      const sandbox = await this.sandboxService.createFromSnapshot(
        {
          snapshot: template.snapshotId,
          labels: {
            'daytona.io/session': 'true',
            'daytona.io/session-template': template.name,
            'daytona.io/session-instance': saved.id,
          },
          env: {
            SESSION_DAEMON_API_IDLE_TTL_SECONDS_HINT: String(idleTtlSec),
          },
        },
        organization,
      )
      saved.sandboxId = sandbox.id
      saved = await this.instanceRepo.save(saved)
      return await this.waitForReady(saved.id)
    } catch (err) {
      this.logger.error(`pool provision failed for instance ${saved.id}: ${err.message}`)
      saved.state = SessionInstanceState.ERROR
      saved.errorReason = err.message
      await this.instanceRepo.save(saved)
      throw err
    }
  }

  private async markActive(inst: SessionInstance): Promise<void> {
    try {
      await this.instanceRepo.update({ id: inst.id }, { lastActiveAt: new Date() })
    } catch (err) {
      this.logger.debug(`markActive(${inst.id}) failed: ${err.message}`)
    }
  }

  /**
   * Reconcile cron — every 30s: roll dead instances, top up to minWarm, scale in idle overflow,
   * prune long-dead ERROR rows.
   */
  @Cron(CronExpression.EVERY_30_SECONDS, { name: 'session-pool-reconcile' })
  async reconcile(): Promise<void> {
    const instances = await this.instanceRepo.find({
      where: { state: SessionInstanceState.READY },
    })
    for (const inst of instances) {
      try {
        await this.reconcileOne(inst)
      } catch (err) {
        this.logger.warn(`reconcile of instance ${inst.id} failed: ${err.message}`)
      }
    }

    try {
      await this.ensureMinWarm()
    } catch (err) {
      this.logger.warn(`ensureMinWarm failed: ${err.message}`)
    }
    try {
      await this.scaleIn()
    } catch (err) {
      this.logger.warn(`scaleIn failed: ${err.message}`)
    }
    try {
      await this.pruneErroredInstances()
    } catch (err) {
      this.logger.warn(`pruneErroredInstances failed: ${err.message}`)
    }
  }

  private async reconcileOne(inst: SessionInstance): Promise<void> {
    if (!inst.sandboxId) return
    const sandbox = await this.sandboxRepo.findOne({ where: { id: inst.sandboxId } })
    // STARTED is the only state in which the runner can route a daemon request. Anything else
    // (stopped, error, destroyed, ...) means the instance is unusable — roll it now rather than
    // serving the SDK a 400 from the runner proxy.
    if (!sandbox || sandbox.state !== SandboxState.STARTED) {
      await this.rollInstance(inst, `sandbox ${inst.sandboxId} not started (state=${sandbox?.state ?? 'missing'})`)
      return
    }
    const tpl = await this.templateRepo.findOne({ where: { id: inst.templateId } })
    if (tpl && tpl.snapshotId !== inst.snapshotId) {
      await this.rollInstance(inst, `snapshot drift: template now points to ${tpl.snapshotId}`)
    }
  }

  /**
   * Top up each in-use (org, template) fleet to `minWarm` READY instances. Only acts on pairs
   * that already have at least one READY instance, so unused templates are never pre-provisioned
   * (warm sandboxes are still created lazily on first acquire).
   */
  private async ensureMinWarm(): Promise<void> {
    const minWarm = this.config.get('session.scale.minWarm') ?? 1
    if (minWarm <= 1) return // default: lazy create already keeps exactly one warm

    const ready = await this.instanceRepo.find({ where: { state: SessionInstanceState.READY } })
    const groups = this.groupByOrgTemplate(ready)
    for (const [, members] of groups) {
      const { organizationId, templateId } = members[0]
      const provisioning = await this.countByState(organizationId, templateId, SessionInstanceState.PROVISIONING)
      const have = members.length + provisioning
      if (have >= minWarm) continue
      const tpl = await this.templateRepo.findOne({ where: { id: templateId } })
      if (!tpl) continue
      // Best-effort: top up one per reconcile tick to avoid bursts (next tick continues).
      const reserved = await this.tryReserveInstance(organizationId, templateId)
      if (reserved) {
        reserved.role = SessionInstanceRole.WARM
        this.provisionReserved(reserved, { id: organizationId } as Organization, tpl).catch((err) =>
          this.logger.warn(`ensureMinWarm provision failed: ${err.message}`),
        )
      }
    }
  }

  /**
   * Scale in idle `overflow` instances while keeping at least `minWarm` per (org, template).
   * An overflow instance is reaped when it is READY, has zero effective load, and has been idle
   * (no pick) for longer than `scaleInIdleSeconds`.
   */
  private async scaleIn(): Promise<void> {
    const minWarm = this.config.get('session.scale.minWarm') ?? 1
    const idleMs = (this.config.get('session.scale.scaleInIdleSeconds') ?? 600) * 1000
    const now = Date.now()

    const ready = await this.instanceRepo.find({ where: { state: SessionInstanceState.READY } })
    const groups = this.groupByOrgTemplate(ready)
    for (const [, members] of groups) {
      let total = members.length
      if (total <= minWarm) continue
      // Reap overflow first; oldest-idle first.
      const overflow = members
        .filter((m) => m.role === SessionInstanceRole.OVERFLOW)
        .sort((a, b) => (a.lastActiveAt?.getTime() ?? 0) - (b.lastActiveAt?.getTime() ?? 0))
      for (const inst of overflow) {
        if (total <= minWarm) break
        const idle = !inst.lastActiveAt || now - inst.lastActiveAt.getTime() > idleMs
        if (!idle) continue
        if ((await this.load.effectiveLoad(inst.id)) > 0) continue
        await this.reapInstance(inst)
        total--
      }
    }
  }

  private async reapInstance(inst: SessionInstance): Promise<void> {
    this.logger.log(`scaling in idle overflow SessionInstance ${inst.id} (sandbox ${inst.sandboxId})`)
    // rollInstance destroys the underlying sandbox when it still exists (see destroySandbox flag).
    await this.rollInstance(inst, 'scaled in (idle overflow)')
  }

  /** Delete ERROR instance rows (and their already-invalid sessions) after a grace period. */
  private async pruneErroredInstances(): Promise<void> {
    const graceMs = (this.config.get('session.scale.scaleInIdleSeconds') ?? 600) * 1000
    const cutoff = new Date(Date.now() - graceMs)
    await this.instanceRepo.delete({ state: SessionInstanceState.ERROR, updatedAt: LessThan(cutoff) })
  }

  private groupByOrgTemplate(instances: SessionInstance[]): Map<string, SessionInstance[]> {
    const groups = new Map<string, SessionInstance[]>()
    for (const inst of instances) {
      const key = `${inst.organizationId}:${inst.templateId}`
      const arr = groups.get(key) ?? []
      arr.push(inst)
      groups.set(key, arr)
    }
    return groups
  }

  /**
   * Acquire-time liveness probe. Reads only the local sandbox row (cheap) and trusts the runner's
   * state replication — covers the dominant case (auto-stop, error, destroy) without an extra hop.
   */
  private async isSandboxLive(inst: SessionInstance): Promise<boolean> {
    if (!inst.sandboxId) return false
    const sandbox = await this.sandboxRepo.findOne({ where: { id: inst.sandboxId } })
    return sandbox?.state === SandboxState.STARTED
  }

  private async rollInstance(inst: SessionInstance, reason: string): Promise<void> {
    this.logger.log(`rolling SessionInstance ${inst.id}: ${reason}`)
    inst.state = SessionInstanceState.ERROR
    inst.errorReason = reason
    await this.instanceRepo.save(inst)
    await this.sessions.markInstanceSessionsInvalid(inst.id)
    // Destroy the underlying sandbox so a rolled instance can't orphan it (pruneErroredInstances
    // only deletes the DB row). Guard on the sandbox row still existing and not already being
    // destroyed, so we don't double-destroy when the sandbox is already dead/gone.
    await this.destroyInstanceSandbox(inst)
  }

  /**
   * Best-effort destroy of an instance's sandbox, only when it still exists and isn't already
   * (being) destroyed — so the dead/missing-sandbox roll paths don't double-destroy.
   */
  private async destroyInstanceSandbox(inst: SessionInstance): Promise<void> {
    if (!inst.sandboxId) return
    try {
      const sandbox = await this.sandboxRepo.findOne({ where: { id: inst.sandboxId } })
      if (!sandbox) return // already gone (missing / hard-deleted)
      if (sandbox.state === SandboxState.DESTROYED || sandbox.desiredState === SandboxDesiredState.DESTROYED) {
        return // already (being) destroyed
      }
      await this.sandboxService.destroy(inst.sandboxId, inst.organizationId)
    } catch (err) {
      this.logger.warn(`destroy of sandbox ${inst.sandboxId} (instance ${inst.id}) failed: ${err.message}`)
    }
  }

  private async waitForReady(instanceId: string): Promise<SessionInstance> {
    const deadline = Date.now() + (this.config.get('session.provisionTimeoutMs') ?? 180000)
    while (Date.now() < deadline) {
      const inst = await this.instanceRepo.findOne({ where: { id: instanceId } })
      if (!inst) throw new NotFoundException(`SessionInstance ${instanceId} disappeared while waiting`)
      if (inst.state === SessionInstanceState.ERROR) {
        throw new Error(`SessionInstance failed: ${inst.errorReason ?? 'unknown'}`)
      }
      if (inst.state === SessionInstanceState.READY) return inst

      if (inst.sandboxId) {
        const sandbox = await this.sandboxRepo.findOne({ where: { id: inst.sandboxId } })
        if (sandbox?.state === SandboxState.STARTED) {
          // Mark READY on sandbox STARTED; the first real exec surfaces a clean failure if the
          // daemon isn't reachable (same semantic as SessionInvalidatedError).
          inst.state = SessionInstanceState.READY
          await this.instanceRepo.save(inst)
          return inst
        }
      }
      await this.sleep(2000)
    }
    throw new Error('session instance provisioning timed out')
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms))
  }
}
