CREATE TABLE "workflow_instances" (
	"id" text PRIMARY KEY NOT NULL,
	"workflow" text NOT NULL,
	"version" integer NOT NULL,
	"status" text NOT NULL,
	"input" jsonb,
	"output" jsonb,
	"error" jsonb,
	"wake_at" bigint,
	"lease_token" text,
	"lease_owner" text,
	"lease_until" bigint,
	"cancel_requested" boolean DEFAULT false NOT NULL,
	"cancel_reason" text,
	"signal_cursor" bigint NOT NULL,
	"runs" integer DEFAULT 0 NOT NULL,
	"created_at" bigint NOT NULL,
	"updated_at" bigint NOT NULL
);
--> statement-breakpoint
CREATE TABLE "workflow_journal" (
	"seq" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "workflow_journal_seq_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"instance_id" text NOT NULL,
	"name" text NOT NULL,
	"entry" jsonb NOT NULL,
	CONSTRAINT "workflow_journal_name" UNIQUE("instance_id","name")
);
--> statement-breakpoint
CREATE TABLE "workflow_signals" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "workflow_signals_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"name" text NOT NULL,
	"key" text,
	"payload" jsonb,
	"created_at" bigint NOT NULL
);
--> statement-breakpoint
CREATE TABLE "workflow_waits" (
	"instance_id" text NOT NULL,
	"position" integer NOT NULL,
	"signal" text NOT NULL,
	"key" text,
	CONSTRAINT "workflow_waits_instance_id_position_pk" PRIMARY KEY("instance_id","position")
);
--> statement-breakpoint
ALTER TABLE "workflow_journal" ADD CONSTRAINT "workflow_journal_instance_id_workflow_instances_id_fk" FOREIGN KEY ("instance_id") REFERENCES "public"."workflow_instances"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "workflow_waits" ADD CONSTRAINT "workflow_waits_instance_id_workflow_instances_id_fk" FOREIGN KEY ("instance_id") REFERENCES "public"."workflow_instances"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "workflow_instances_due" ON "workflow_instances" USING btree ("wake_at") WHERE "workflow_instances"."wake_at" IS NOT NULL;--> statement-breakpoint
CREATE INDEX "workflow_instances_created" ON "workflow_instances" USING btree ("created_at","id");--> statement-breakpoint
CREATE INDEX "workflow_signals_lookup" ON "workflow_signals" USING btree ("name","key","id");--> statement-breakpoint
CREATE INDEX "workflow_waits_signal" ON "workflow_waits" USING btree ("signal","key");