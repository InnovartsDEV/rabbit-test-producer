#!/bin/sh
set -e

# Aplica las migraciones pendientes antes de arrancar (idempotente)
pnpm exec prisma migrate deploy

exec "$@"
