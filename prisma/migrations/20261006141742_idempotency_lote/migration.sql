-- AlterTable
ALTER TABLE "Documento" ADD COLUMN     "idempotencyKey" VARCHAR(255),
ADD COLUMN     "loteId" UUID;

-- CreateIndex
CREATE UNIQUE INDEX "Documento_idempotencyKey_key" ON "Documento"("idempotencyKey");

-- CreateIndex
CREATE INDEX "Documento_loteId_idx" ON "Documento"("loteId");

