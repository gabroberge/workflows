CREATE TABLE "orders" (
	"id" text PRIMARY KEY NOT NULL,
	"user_id" text NOT NULL,
	"items" jsonb NOT NULL,
	"total" integer NOT NULL,
	"status" text NOT NULL
);
