import { AmqpConnection } from '@golevelup/nestjs-rabbitmq';
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import type { Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { CHUNK_NAS, EXCHANGE, RK_GUARDAR_NAS_LOTE } from './outbox.constants';
import type { NasItemMsg, NasLoteMsg } from './outbox.constants';

const POLL_MS = 1000;
const BATCH_SIZE = 200;
const MAX_ATTEMPTS = 5;

interface PendingRow {
  id: string;
  destino: 'NAS' | 'BD';
  loteId: string | null;
  routingKey: string;
  payload: Omit<NasItemMsg, 'eventId' | 'reintento'>;
  attempts: number;
  reintentos: number;
}

// Eventos creados antes de quitar tempPath del payload: la ruta del servidor no sale de aqui
function sinRutaLocal<T extends object>(payload: T): T {
  const { tempPath: _omitida, ...resto } = payload as T & { tempPath?: string };
  return resto as T;
}

/**
 * Lee eventos PENDING del outbox y los publica en RabbitMQ.
 *  - BD: un mensaje por evento.
 *  - NAS: trozos de hasta CHUNK_NAS documentos del mismo lote en un solo mensaje
 *    (el consumer los sube con una sola conexion SFTP). El estado sigue siendo
 *    por documento: cada evento tiene su propio resultado.
 */
@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelayService.name);
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly amqp: AmqpConnection,
  ) {}

  onModuleInit() {
    this.timer = setInterval(() => void this.tick(), POLL_MS);
  }

  onModuleDestroy() {
    clearInterval(this.timer);
  }

  private async tick() {
    if (this.running) return;
    this.running = true;
    try {
      await this.relayBatch();
    } catch (error) {
      this.logger.error('Fallo el ciclo del relay', error);
    } finally {
      this.running = false;
    }
  }

  private async relayBatch() {
    await this.prisma.$transaction(
      async (tx) => {
        // SKIP LOCKED: varias instancias del publisher no se pisan
        const rows = await tx.$queryRaw<PendingRow[]>`
          SELECT e.id, e.destino, d."loteId", e."routingKey", e.payload,
                 e.attempts, d.reintentos
          FROM "OutboxEvent" e
          JOIN "Documento" d ON d.id = e."documentoId"
          WHERE e.status = 'PENDING'::"OutboxStatus"
          ORDER BY e."createdAt"
          LIMIT ${BATCH_SIZE}
          FOR UPDATE OF e SKIP LOCKED`;

        for (const row of rows.filter((r) => r.destino === 'BD')) {
          await this.publicar(
            tx,
            [row],
            row.routingKey,
            { ...row.payload, eventId: row.id, reintento: row.reintentos },
          );
        }

        for (const trozo of this.trozosNas(rows.filter((r) => r.destino === 'NAS'))) {
          const msg: NasLoteMsg = {
            loteId: trozo[0].loteId ?? 'sin-lote',
            items: trozo.map((r) => ({
              ...sinRutaLocal(r.payload),
              eventId: r.id,
              reintento: r.reintentos,
            })),
          };
          await this.publicar(tx, trozo, RK_GUARDAR_NAS_LOTE, msg);
        }
      },
      { timeout: 30_000 },
    );
  }

  /** Agrupa por lote y parte en trozos de CHUNK_NAS. */
  private trozosNas(rows: PendingRow[]): PendingRow[][] {
    const porLote = new Map<string, PendingRow[]>();
    for (const r of rows) {
      const k = r.loteId ?? 'sin-lote';
      porLote.set(k, [...(porLote.get(k) ?? []), r]);
    }
    const trozos: PendingRow[][] = [];
    for (const grupo of porLote.values()) {
      for (let i = 0; i < grupo.length; i += CHUNK_NAS) {
        trozos.push(grupo.slice(i, i + CHUNK_NAS));
      }
    }
    return trozos;
  }

  /** Publica un mensaje que cubre `rows` y marca todos SENT (o registra el fallo). */
  private async publicar(
    tx: Prisma.TransactionClient,
    rows: PendingRow[],
    routingKey: string,
    mensaje: unknown,
  ) {
    try {
      await this.amqp.publish(EXCHANGE, routingKey, mensaje, {
        persistent: true,
        messageId: randomUUID(),
      });
      await tx.outboxEvent.updateMany({
        where: { id: { in: rows.map((r) => r.id) } },
        data: { status: 'SENT', sentAt: new Date(), attempts: { increment: 1 } },
      });
      this.logger.log(`Publicado ${routingKey} (${rows.length} evento/s)`);
    } catch (error) {
      for (const row of rows) {
        const attempts = row.attempts + 1;
        await tx.outboxEvent.update({
          where: { id: row.id },
          data: {
            attempts,
            lastError: String(error),
            status: attempts >= MAX_ATTEMPTS ? 'FAILED' : 'PENDING',
          },
        });
      }
      this.logger.warn(`Fallo al publicar ${routingKey} (${rows.length} evento/s)`);
    }
  }
}
