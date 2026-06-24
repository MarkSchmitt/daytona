/*
 * Copyright Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */
import { MigrationInterface, QueryRunner } from 'typeorm'

/**
 * Tenant-consistency composite FK between `session` and `session_instance`.
 *
 * The original `FK_session_instance_id` only constrained `session.instanceId` -> `session_instance.id`,
 * so nothing at the DB level prevented a session from referencing an instance owned by a DIFFERENT
 * organization. The app layer enforces same-org resolution, but there was no DB guarantee.
 *
 * This is a post-deploy (contract) migration: it hardens an invariant the deployed code already
 * maintains, so it must run only after the new API is live.
 *
 * Steps:
 *   1. Add a UNIQUE constraint on session_instance(organizationId, id) so the pair can be the target
 *      of a composite FK. (`id` is already the PK, so the pair is trivially unique — this is the
 *      Postgres requirement that a composite FK reference a unique/PK column set.)
 *   2. Replace the single-column FK with a composite FK on session(organizationId, instanceId)
 *      REFERENCING session_instance(organizationId, id), keeping ON DELETE CASCADE to match the
 *      original constraint.
 *
 * Why REPLACE the single-column FK rather than keep both:
 *   - `session.organizationId` and `session.instanceId` are both NOT NULL (see the create migration
 *     1778367241000 and Session entity). The composite FK uses the default MATCH SIMPLE semantics,
 *     which only skip enforcement when one of the referencing columns is NULL — that can never
 *     happen here, so the composite FK ALWAYS fires.
 *   - Any row satisfying the composite check (a session_instance with the same org AND id exists)
 *     necessarily satisfies the single-column check (a session_instance with that id exists).
 *     Keeping both would be a redundant, overlapping constraint. Since organizationId is guaranteed
 *     non-null we can safely drop the single-column FK without losing referential integrity.
 */
export class Migration1781700000001 implements MigrationInterface {
  name = 'Migration1781700000001'

  public async up(queryRunner: QueryRunner): Promise<void> {
    // 1. Unique target for the composite FK (id is PK, so the pair is trivially unique).
    await queryRunner.query(
      `ALTER TABLE "session_instance" ADD CONSTRAINT "UQ_session_instance_org_id" UNIQUE ("organizationId", "id")`,
    )

    // 2. Drop the single-column FK (superseded by the composite one — organizationId is NOT NULL).
    await queryRunner.query(`ALTER TABLE "session" DROP CONSTRAINT "FK_session_instance_id"`)

    // 3. Composite, tenant-consistent FK. MATCH SIMPLE is fine: both columns are NOT NULL.
    await queryRunner.query(`
      ALTER TABLE "session" ADD CONSTRAINT "FK_session_org_instance"
        FOREIGN KEY ("organizationId", "instanceId")
        REFERENCES "session_instance"("organizationId", "id") ON DELETE CASCADE
    `)
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "session" DROP CONSTRAINT "FK_session_org_instance"`)
    await queryRunner.query(`
      ALTER TABLE "session" ADD CONSTRAINT "FK_session_instance_id"
        FOREIGN KEY ("instanceId") REFERENCES "session_instance"("id") ON DELETE CASCADE
    `)
    await queryRunner.query(`ALTER TABLE "session_instance" DROP CONSTRAINT "UQ_session_instance_org_id"`)
  }
}
