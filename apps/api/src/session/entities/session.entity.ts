/*
 * Copyright Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { Column, CreateDateColumn, Entity, Index, JoinColumn, ManyToOne, PrimaryGeneratedColumn } from 'typeorm'
import { SessionInstance } from './session-instance.entity'
import { SessionState } from '../enums/session-state.enum'

/**
 * Session owns context identity for the API. Its `id` is the user-facing identifier AND
 * the id passed verbatim to the in-sandbox session-daemon on `POST /sessions`. One row, one
 * identity, one lookup.
 *
 * `lastUsedAt` is bumped on every successful resolve and feeds the idle-TTL GC sweep. The pool
 * reconciler bulk-marks rows INVALID when an instance is rolled; the GC marks them EXPIRED on
 * idle/absolute TTL.
 */
@Entity('session')
@Index('session_org_id_idx', ['organizationId', 'id'])
@Index('session_instance_id_state_idx', ['instanceId', 'state'])
@Index('session_state_lastusedat_idx', ['state', 'lastUsedAt'])
export class Session {
  @PrimaryGeneratedColumn('uuid')
  id: string

  @Column({ type: 'uuid' })
  organizationId: string

  @Column({ type: 'uuid' })
  instanceId: string

  @ManyToOne(() => SessionInstance, { eager: false, onDelete: 'CASCADE' })
  @JoinColumn({ name: 'instanceId' })
  instance?: SessionInstance

  @Column()
  language: string

  @Column({ type: 'text', nullable: true })
  cwd?: string

  @Column({
    type: 'enum',
    enum: SessionState,
    default: SessionState.ACTIVE,
  })
  state: SessionState = SessionState.ACTIVE

  @Column({ type: 'timestamp with time zone', nullable: true })
  invalidatedAt?: Date

  @Column({ type: 'timestamp with time zone', nullable: true })
  expiredAt?: Date

  @CreateDateColumn({ type: 'timestamp with time zone' })
  createdAt: Date

  @Column({ type: 'timestamp with time zone' })
  lastUsedAt: Date
}
