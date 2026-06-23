/*
 * Copyright Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */
import { MigrationInterface, QueryRunner } from 'typeorm'

/**
 * Session scale-out: allow many SessionInstance rows per (organizationId, templateId).
 *
 * Drops the single-instance unique constraint, adds a composite lookup index, and adds the
 * `role` (warm | overflow) + `lastActiveAt` columns the autoscaler/scale-in logic needs.
 */
export class Migration1781700000000 implements MigrationInterface {
  name = 'Migration1781700000000'

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`CREATE TYPE "session_instance_role_enum" AS ENUM('warm', 'overflow')`)
    await queryRunner.query(
      `ALTER TABLE "session_instance" ADD "role" "session_instance_role_enum" NOT NULL DEFAULT 'warm'`,
    )
    await queryRunner.query(`ALTER TABLE "session_instance" ADD "lastActiveAt" TIMESTAMP WITH TIME ZONE`)

    // Drop the one-instance-per-(org,template) invariant; replace with a non-unique composite index.
    await queryRunner.query(`ALTER TABLE "session_instance" DROP CONSTRAINT "session_instance_org_template_uidx"`)
    await queryRunner.query(
      `CREATE INDEX "session_instance_org_template_state_idx" ON "session_instance" ("organizationId", "templateId", "state")`,
    )
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX IF EXISTS "session_instance_org_template_state_idx"`)
    // Restoring the unique constraint requires the table to already satisfy it; callers running
    // down() are responsible for collapsing duplicate instances first.
    await queryRunner.query(
      `ALTER TABLE "session_instance" ADD CONSTRAINT "session_instance_org_template_uidx" UNIQUE ("organizationId", "templateId")`,
    )
    await queryRunner.query(`ALTER TABLE "session_instance" DROP COLUMN "lastActiveAt"`)
    await queryRunner.query(`ALTER TABLE "session_instance" DROP COLUMN "role"`)
    await queryRunner.query(`DROP TYPE IF EXISTS "session_instance_role_enum"`)
  }
}
