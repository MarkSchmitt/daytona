/*
 * Copyright Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { MigrationInterface, QueryRunner } from 'typeorm'

/**
 * Creates the three new session tables (session_template, session_instance, session).
 * No existing entity is modified — these tables only reference Snapshot via FK.
 */
export class Migration1778367241000 implements MigrationInterface {
  name = 'Migration1778367241000'

  public async up(queryRunner: QueryRunner): Promise<void> {
    // Enums
    await queryRunner.query(`CREATE TYPE "session_instance_state_enum" AS ENUM('provisioning', 'ready', 'error')`)
    await queryRunner.query(`CREATE TYPE "session_state_enum" AS ENUM('active', 'invalid', 'expired')`)

    // session_template
    await queryRunner.query(`
      CREATE TABLE "session_template" (
        "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
        "name" varchar NOT NULL,
        "organizationId" uuid,
        "general" boolean NOT NULL DEFAULT false,
        "description" text,
        "languages" text[] NOT NULL DEFAULT ARRAY[]::text[],
        "packages" text[],
        "snapshotId" uuid NOT NULL,
        "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        CONSTRAINT "PK_session_template" PRIMARY KEY ("id"),
        CONSTRAINT "UQ_session_template_snapshot" UNIQUE ("snapshotId"),
        CONSTRAINT "FK_session_template_snapshot"
          FOREIGN KEY ("snapshotId") REFERENCES "snapshot"("id") ON DELETE RESTRICT
      )
    `)
    await queryRunner.query(`
      CREATE UNIQUE INDEX "session_template_org_name_uidx"
      ON "session_template" (COALESCE("organizationId", '00000000-0000-0000-0000-000000000000'), "name")
    `)
    await queryRunner.query(`CREATE INDEX "session_template_org_id_idx" ON "session_template" ("organizationId")`)
    await queryRunner.query(`CREATE INDEX "session_template_general_idx" ON "session_template" ("general")`)

    // session_instance
    await queryRunner.query(`
      CREATE TABLE "session_instance" (
        "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
        "organizationId" uuid NOT NULL,
        "templateId" uuid NOT NULL,
        "snapshotId" uuid NOT NULL,
        "sandboxId" uuid,
        "state" "session_instance_state_enum" NOT NULL DEFAULT 'provisioning',
        "errorReason" text,
        "lastUsedAt" TIMESTAMP WITH TIME ZONE,
        "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        CONSTRAINT "PK_session_instance" PRIMARY KEY ("id"),
        CONSTRAINT "session_instance_org_template_uidx" UNIQUE ("organizationId", "templateId"),
        CONSTRAINT "FK_session_instance_template"
          FOREIGN KEY ("templateId") REFERENCES "session_template"("id") ON DELETE CASCADE,
        CONSTRAINT "FK_session_instance_snapshot"
          FOREIGN KEY ("snapshotId") REFERENCES "snapshot"("id") ON DELETE RESTRICT
      )
    `)
    await queryRunner.query(`CREATE INDEX "session_instance_state_idx" ON "session_instance" ("state")`)
    await queryRunner.query(`CREATE INDEX "session_instance_sandbox_idx" ON "session_instance" ("sandboxId")`)

    // session
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
        CONSTRAINT "FK_session_instance_id"
          FOREIGN KEY ("instanceId") REFERENCES "session_instance"("id") ON DELETE CASCADE
      )
    `)
    await queryRunner.query(`CREATE INDEX "session_org_id_idx" ON "session" ("organizationId", "id")`)
    await queryRunner.query(`CREATE INDEX "session_instance_id_state_idx" ON "session" ("instanceId", "state")`)
    await queryRunner.query(`CREATE INDEX "session_state_lastusedat_idx" ON "session" ("state", "lastUsedAt")`)
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TABLE IF EXISTS "session"`)
    await queryRunner.query(`DROP TABLE IF EXISTS "session_instance"`)
    await queryRunner.query(`DROP TABLE IF EXISTS "session_template"`)
    await queryRunner.query(`DROP TYPE IF EXISTS "session_state_enum"`)
    await queryRunner.query(`DROP TYPE IF EXISTS "session_instance_state_enum"`)
  }
}
