/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm'
import { SessionTemplate } from './session-template.entity'
import { Snapshot } from '../../sandbox/entities/snapshot.entity'
import { SessionInstanceState } from '../enums/session-instance-state.enum'
import { SessionInstanceRole } from '../enums/session-instance-role.enum'

/**
 * SessionInstance represents a single warm sandbox backing one (organization, template) pair.
 * The pool service owns lifecycle: PROVISIONING → READY → (rolled on snapshot drift, sandbox
 * death, or autostop).
 *
 * Scale-out: there can now be MANY instances per (organizationId, templateId) — the old unique
 * constraint is gone. `role` distinguishes the always-on `warm` floor from `overflow` instances
 * that the autoscaler adds under load and reaps first when idle. `lastActiveAt` tracks when the
 * instance last served a request, driving scale-in.
 *
 * `snapshotId` is denormalized from the template at instance-create time so the pool reconciler
 * can detect drift (instance.snapshotId != template.snapshotId) without an extra join.
 */
@Entity('session_instance')
@Index('session_instance_org_template_state_idx', ['organizationId', 'templateId', 'state'])
@Index('session_instance_state_idx', ['state'])
@Index('session_instance_sandbox_idx', ['sandboxId'])
export class SessionInstance {
  @PrimaryGeneratedColumn('uuid')
  id: string

  @Column({ type: 'uuid' })
  organizationId: string

  @Column({ type: 'uuid' })
  templateId: string

  @ManyToOne(() => SessionTemplate, { eager: false, onDelete: 'CASCADE' })
  @JoinColumn({ name: 'templateId' })
  template?: SessionTemplate

  @Column({ type: 'uuid' })
  snapshotId: string

  @ManyToOne(() => Snapshot, { eager: false, onDelete: 'RESTRICT' })
  @JoinColumn({ name: 'snapshotId' })
  snapshot?: Snapshot

  @Column({ type: 'uuid', nullable: true })
  sandboxId?: string

  @Column({
    type: 'enum',
    enum: SessionInstanceState,
    default: SessionInstanceState.PROVISIONING,
  })
  state: SessionInstanceState = SessionInstanceState.PROVISIONING

  @Column({ type: 'text', nullable: true })
  errorReason?: string

  @Column({
    type: 'enum',
    enum: SessionInstanceRole,
    default: SessionInstanceRole.WARM,
  })
  role: SessionInstanceRole = SessionInstanceRole.WARM

  @Column({ type: 'timestamp with time zone', nullable: true })
  lastUsedAt?: Date

  /**
   * When this instance last served (or was selected to serve) a request. Distinct from
   * `lastUsedAt` (which the legacy single-pool path bumped): the scheduler stamps this on
   * every pick so scale-in only reaps `overflow` instances that have been idle long enough.
   */
  @Column({ type: 'timestamp with time zone', nullable: true })
  lastActiveAt?: Date

  @CreateDateColumn({ type: 'timestamp with time zone' })
  createdAt: Date

  @UpdateDateColumn({ type: 'timestamp with time zone' })
  updatedAt: Date
}
