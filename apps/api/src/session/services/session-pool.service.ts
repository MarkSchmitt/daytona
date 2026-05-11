/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { Injectable, Logger, NotFoundException } from '@nestjs/common'
import { Cron, CronExpression } from '@nestjs/schedule'
import { InjectRepository } from '@nestjs/typeorm'
import { Repository } from 'typeorm'
import { SessionInstance } from '../entities/session-instance.entity'
import { SessionTemplate } from '../entities/session-template.entity'
import { SessionInstanceState } from '../enums/session-instance-state.enum'
import { SandboxService } from '../../sandbox/services/sandbox.service'
import { Organization } from '../../organization/entities/organization.entity'
import { RedisLockProvider } from '../../sandbox/common/redis-lock.provider'
import { TypedConfigService } from '../../config/typed-config.service'
import { SessionRepository } from './session-repository.service'
import { SandboxState } from '../../sandbox/enums/sandbox-state.enum'
import { Sandbox } from '../../sandbox/entities/sandbox.entity'

const POOL_LOCK_TTL_SEC = 120

/**
 * SessionPoolService owns the warm-pool lifecycle for SessionInstance rows.
 *
 * Invariants:
 *  - Exactly one SessionInstance per (organizationId, templateId) at a time.
 *  - When a sandbox dies or its snapshot drifts, the instance is rolled and all dependent
 *    Session rows are atomically marked INVALID (so dangling contexts can never be
 *    silently routed to a fresh sandbox).
 *  - Sandbox creation passes SESSION_DAEMON_API_IDLE_TTL_SECONDS_HINT in env so the in-sandbox
 *    daemon can warn at boot if its idle TTL is < 1.5× the API's. The hint is captured at
 *    sandbox-create time and does NOT propagate to running daemons if the API's TTL is later
 *    changed — that's intentional (this warning is for deployment-time misconfiguration).
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
  ) {}

  async acquire(orgId: string, organization: Organization, template: SessionTemplate): Promise<SessionInstance> {
    // Fast path: existing READY instance.
    let instance = await this.instanceRepo.findOne({
      where: { organizationId: orgId, templateId: template.id },
    })
    if (instance && instance.state === SessionInstanceState.READY) {
      // Verify drift: if the instance's snapshotId no longer matches the template, roll it.
      if (instance.snapshotId !== template.snapshotId) {
        await this.rollInstance(instance, 'snapshot drift detected at acquire-time')
        instance = null as unknown as SessionInstance
      } else if (!(await this.isSandboxLive(instance))) {
        // Underlying sandbox auto-stopped / errored between requests. The 30s
        // reconcile cron would catch this eventually, but we'd rather not let
        // the SDK eat a guaranteed 400 from the runner proxy in the meantime.
        await this.rollInstance(instance, `sandbox ${instance.sandboxId} no longer started at acquire-time`)
        instance = null as unknown as SessionInstance
      } else {
        return instance
      }
    }

    if (instance && instance.state === SessionInstanceState.PROVISIONING) {
      return this.waitForReady(instance.id)
    }

    const lockKey = `session:pool:${orgId}:${template.id}`
    if (!(await this.lockProvider.lock(lockKey, POOL_LOCK_TTL_SEC))) {
      // Another node is provisioning; poll the row.
      return this.waitForExistingInstance(orgId, template.id)
    }

    try {
      // Re-read after lock, in case another node finished while we waited.
      instance = await this.instanceRepo.findOne({
        where: { organizationId: orgId, templateId: template.id },
      })
      if (instance && instance.state === SessionInstanceState.READY) {
        return instance
      }

      if (!instance) {
        instance = this.instanceRepo.create({
          organizationId: orgId,
          templateId: template.id,
          snapshotId: template.snapshotId,
          state: SessionInstanceState.PROVISIONING,
        })
        instance = await this.instanceRepo.save(instance)
      }

      // Create the sandbox via the existing public surface — no runner/daemon edits needed.
      const idleTtlSec = this.config.get('session.context.idleTtlSeconds') ?? 3600
      const sandbox = await this.sandboxService.createFromSnapshot(
        {
          snapshot: template.snapshotId,
          labels: {
            'daytona.io/session': 'true',
            'daytona.io/session-template': template.name,
            'daytona.io/session-instance': instance.id,
          },
          env: {
            SESSION_DAEMON_API_IDLE_TTL_SECONDS_HINT: String(idleTtlSec),
          },
        },
        organization,
      )
      instance.sandboxId = sandbox.id
      instance.state = SessionInstanceState.PROVISIONING
      await this.instanceRepo.save(instance)

      const ready = await this.waitForReady(instance.id)
      return ready
    } catch (err) {
      this.logger.error(`pool acquire failed: ${err.message}`)
      if (instance) {
        instance.state = SessionInstanceState.ERROR
        instance.errorReason = err.message
        await this.instanceRepo.save(instance)
      }
      throw err
    } finally {
      await this.lockProvider.unlock(lockKey).catch(() => undefined)
    }
  }

  async findInstance(orgId: string, templateId: string): Promise<SessionInstance | null> {
    return this.instanceRepo.findOne({ where: { organizationId: orgId, templateId } })
  }

  /**
   * Reconcile cron — detects dead sandboxes and rolls instances. Runs every 30s.
   */
  @Cron(CronExpression.EVERY_30_SECONDS, { name: 'session-pool-reconcile' })
  async reconcile(): Promise<void> {
    const instances = await this.instanceRepo.find({
      where: { state: SessionInstanceState.READY },
    })
    if (instances.length === 0) return

    for (const inst of instances) {
      try {
        await this.reconcileOne(inst)
      } catch (err) {
        this.logger.warn(`reconcile of instance ${inst.id} failed: ${err.message}`)
      }
    }
  }

  private async reconcileOne(inst: SessionInstance): Promise<void> {
    if (!inst.sandboxId) return
    const sandbox = await this.sandboxRepo.findOne({ where: { id: inst.sandboxId } })
    // STARTED is the only state in which the runner can route a daemon request.
    // Anything else (stopped, stopping, archiving, error, build_failed, unknown,
    // destroyed/destroying, archived) means the instance is unusable for the
    // pool's purpose — roll it now rather than serving the SDK a 400 from the
    // runner proxy ("failed to resolve container IP"). Transient "almost-ready"
    // states (starting, restoring, creating, pulling_snapshot, etc.) would
    // normally have been gated by PROVISIONING; an instance landing here in
    // READY with one of those states points at a broken transition we should
    // also roll out of.
    if (!sandbox || sandbox.state !== SandboxState.STARTED) {
      await this.rollInstance(inst, `sandbox ${inst.sandboxId} not started (state=${sandbox?.state ?? 'missing'})`)
      return
    }
    // Snapshot-drift check: template's current snapshotId vs the instance's frozen snapshotId.
    const tpl = await this.templateRepo.findOne({ where: { id: inst.templateId } })
    if (tpl && tpl.snapshotId !== inst.snapshotId) {
      await this.rollInstance(inst, `snapshot drift: template now points to ${tpl.snapshotId}`)
    }
  }

  /**
   * Acquire-time liveness probe. We only check the local sandbox row — the
   * cheaper signal — and trust the runner's own state replication. A separate
   * runner-side healthz would catch network partitions but at the cost of an
   * extra hop on every hot-path call; the sandbox row covers the dominant
   * case (auto-stop, error, destroy) without that overhead.
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
  }

  private async waitForExistingInstance(orgId: string, templateId: string): Promise<SessionInstance> {
    const deadline = Date.now() + (this.config.get('session.provisionTimeoutMs') ?? 180000)
    while (Date.now() < deadline) {
      const inst = await this.instanceRepo.findOne({ where: { organizationId: orgId, templateId } })
      if (inst && inst.state === SessionInstanceState.READY) return inst
      if (inst && inst.state === SessionInstanceState.ERROR) {
        throw new Error(`session instance is in ERROR state: ${inst.errorReason ?? 'unknown'}`)
      }
      await this.sleep(2000)
    }
    throw new NotFoundException('Timed out waiting for session instance to become ready.')
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

      // Drive PROVISIONING → READY by checking sandbox state + healthz.
      if (inst.sandboxId) {
        const sandbox = await this.sandboxRepo.findOne({ where: { id: inst.sandboxId } })
        if (sandbox?.state === SandboxState.STARTED) {
          // The /healthz check will be wired by SessionService once the in-sandbox daemon is
          // running; for now we mark READY when sandbox reports STARTED. The first real exec
          // will surface a clean failure if the daemon isn't reachable, which is the same
          // semantic as SessionInvalidatedError.
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
