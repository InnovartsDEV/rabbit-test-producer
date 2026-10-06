# syntax=docker/dockerfile:1

# ---------- build: compila TypeScript y genera el cliente de Prisma ----------
FROM node:22-bookworm-slim AS build
RUN npm install -g pnpm@12.9.1
WORKDIR /app

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY . .
RUN DATABASE_URL="postgresql://build:build@localhost:5432/build" pnpm exec prisma generate \
 && pnpm build

# ---------- runtime: solo dependencias de produccion ----------
FROM node:22-bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends openssl ca-certificates wget \
 && rm -rf /var/lib/apt/lists/* \
 && npm install -g pnpm@12.9.1
ENV NODE_ENV=production
WORKDIR /app

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile --prod

# schema + config de Prisma: generate para el cliente, migrate deploy al arrancar
COPY prisma ./prisma
COPY prisma.config.ts ./
RUN DATABASE_URL="postgresql://build:build@localhost:5432/build" pnpm exec prisma generate

COPY --from=build /app/dist ./dist
COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh \
 && mkdir -p /app/tmp-uploads \
 && chown -R node:node /app

# tmp-uploads guarda los archivos hasta que la NAS confirma: debe persistir
VOLUME ["/app/tmp-uploads"]
USER node
EXPOSE 3000

HEALTHCHECK --interval=15s --timeout=5s --start-period=40s --retries=5 \
  CMD wget -qO- "http://localhost:${PORT:-3000}/outbox/stats" > /dev/null || exit 1

ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["node", "dist/src/main"]
