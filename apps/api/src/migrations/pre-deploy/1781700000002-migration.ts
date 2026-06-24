/*
 * Copyright Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */
import { MigrationInterface, QueryRunner } from 'typeorm'

/**
 * Move Session and SessionInstance off Postgres onto Redis.
 *
 * Both are now stored entirely in Redis (SessionRepository / SessionInstanceStore): contexts are
 * short-TTL and the warm-instance pool keys real sandbox lifecycle that the orphan-sandbox
 * reconciler protects against a Redis wipe. This migration drops the now-unused `session` and
 * `session_instance` tables and their enum types.
 *
 * `session_template` is intentionally LEFT IN PLACE — it remains durable Postgres config
 * (FK to `snapshot`, seeded by migration), so its table, indexes, enum-free schema, and seed row
 * are untouched.
 */
export class Migration1781700000002 implements MigrationInterface {
  name = 'Migration1781700000002'

  public async up(queryRunner: QueryRunner): Promise<void> {
    // `session` references `session_instance` (composite FK), so drop it first. CASCADE clears the
    // dependent constraints/indexes.
    await queryRunner.query(`DROP TABLE IF EXISTS "session" CASCADE`)
    await queryRunner.query(`DROP TABLE IF EXISTS "session_instance" CASCADE`)
    await queryRunner.query(`DROP TYPE IF EXISTS "session_state_enum"`)
    await queryRunner.query(`DROP TYPE IF EXISTS "session_instance_state_enum"`)
    await queryRunner.query(`DROP TYPE IF EXISTS "session_instance_role_enum"`)
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Recreate the tables as they stood after migrations 1778367241000 + 1781700000000 +
    // 1781700000001 (scale-out columns + tenant-consistency composite FK). Data is not restored —
    // it lived in Redis after the up() ran.
    await queryRunner.query(`CREATE TYPE "session_instance_state_enum" AS ENUM('provisioning', 'ready', 'error')`)
    await queryRunner.query(`CREATE TYPE "session_instance_role_enum" AS ENUM('warm', 'overflow')`)
    await queryRunner.query(`CREATE TYPE "session_state_enum" AS ENUM('active', 'invalid', 'expired')`)

    await queryRunner.query(`
      CREATE TABLE "session_instance" (
        "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
        "organizationId" uuid NOT NULL,
        "templateId" uuid NOT NULL,
        "snapshotId" uuid NOT NULL,
        "sandboxId" uuid,
        "state" "session_instance_state_enum" NOT NULL DEFAULT 'provisioning',
        "errorReason" text,
        "role" "session_instance_role_enum" NOT NULL DEFAULT 'warm',
        "lastUsedAt" TIMESTAMP WITH TIME ZONE,
        "lastActiveAt" TIMESTAMP WITH TIME ZONE,
        "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        CONSTRAINT "PK_session_instance" PRIMARY KEY ("id"),
        CONSTRAINT "UQ_session_instance_org_id" UNIQUE ("organizationId", "id"),
        CONSTRAINT "FK_session_instance_template"
          FOREIGN KEY ("templateId") REFERENCES "session_template"("id") ON DELETE CASCADE,
        CONSTRAINT "FK_session_instance_snapshot"
          FOREIGN KEY ("snapshotId") REFERENCES "snapshot"("id") ON DELETE RESTRICT
      )
    `)
    await queryRunner.query(`CREATE INDEX "session_instance_state_idx" ON "session_instance" ("state")`)
    await queryRunner.query(`CREATE INDEX "session_instance_sandbox_idx" ON "session_instance" ("sandboxId")`)
    await queryRunner.query(
      `CREATE INDEX "session_instance_org_template_state_idx" ON "session_instance" ("organizationId", "templateId", "state")`,
    )

    await queryRunner.query(`
      CREATE TABLE "session" (
        "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
        "organizationId" uuid NOT NULL,
        "instanceId" uuid NOT NULL,
        "language" varchar NOT NULL,
        "cwd" text,
        "state" "session_state_enum" NOT NULL DEFAULT 'active',
        "invalidatedAt" TIMESTAMP WITH TIME ZONE,
        "expiredAt" TIMESTAMP WITH TIME ZONE,
        "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        "lastUsedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        CONSTRAINT "PK_session" PRIMARY KEY ("id"),
        CONSTRAINT "FK_session_org_instance"
          FOREIGN KEY ("organizationId", "instanceId")
          REFERENCES "session_instance"("organizationId", "id") ON DELETE CASCADE
      )
    `)
    await queryRunner.query(`CREATE INDEX "session_org_id_idx" ON "session" ("organizationId", "id")`)
    await queryRunner.query(`CREATE INDEX "session_instance_id_state_idx" ON "session" ("instanceId", "state")`)
    await queryRunner.query(`CREATE INDEX "session_state_lastusedat_idx" ON "session" ("state", "lastUsedAt")`)
  }
}
