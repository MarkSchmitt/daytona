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
  Unique,
  UpdateDateColumn,
} from 'typeorm'
import { SessionTemplate } from './session-template.entity'
import { Snapshot } from '../../sandbox/entities/snapshot.entity'
import { SessionInstanceState } from '../enums/session-instance-state.enum'

/**
 * SessionInstance represents a single warm sandbox dedicated to one (organization, template)
 * pair. The pool service owns lifecycle: PROVISIONING → READY → (rolled on snapshot drift,
 * sandbox death, or autostop).
 *
 * `snapshotId` is denormalized from the template at instance-create time so the pool reconciler
 * can detect drift (instance.snapshotId != template.snapshotId) without an extra join.
 */
@Entity('session_instance')
@Unique('session_instance_org_template_uidx', ['organizationId', 'templateId'])
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

  @Column({ type: 'timestamp with time zone', nullable: true })
  lastUsedAt?: Date

  @CreateDateColumn({ type: 'timestamp with time zone' })
  createdAt: Date

  @UpdateDateColumn({ type: 'timestamp with time zone' })
  updatedAt: Date
}
