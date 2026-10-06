/*
  Warnings:

  - The values [VERDADERO] on the enum `STATUS` will be removed. If these variants are still used in the database, this will fail.

*/
-- AlterEnum
BEGIN;
CREATE TYPE "STATUS_new" AS ENUM ('VERDADERO_2', 'FALSO', 'FALSO_2');
ALTER TYPE "STATUS" RENAME TO "STATUS_old";
ALTER TYPE "STATUS_new" RENAME TO "STATUS";
DROP TYPE "public"."STATUS_old";
COMMIT;
