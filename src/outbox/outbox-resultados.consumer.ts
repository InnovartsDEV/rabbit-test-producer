import {
  MessageHandlerErrorBehavior,
  RabbitSubscribe,
} from '@golevelup/nestjs-rabbitmq';
import { Injectable, Logger } from '@nestjs/common';
import { unlink } from 'node:fs/promises';
import { PrismaService } from '../prisma/prisma.service';
import { EXCHANGE, RESULTADOS_QUEUE, RK_RESULTADO } from './outbox.constants';
import type { ResultadoMsg } from './outbox.constants';

/** Recibe la respuesta de cada consumer y cierra el ciclo del evento. */
@Injectable()
export class OutboxResultadosConsumer {
  private readonly logger = new Logger(OutboxResultadosConsumer.name);

  constructor(private readonly prisma: PrismaService) {}

  @RabbitSubscribe({
    exchange: EXCHANGE,
    routingKey: RK_RESULTADO,
    queue: RESULTADOS_QUEUE,
    queueOptions: { durable: true },
    errorBehavior: MessageHandlerErrorBehavior.NACK,
  })
  async onResultado(msg: ResultadoMsg) {
    // Transiciones solo hacia adelante: un resultado duplicado o tardio
    // (p.ej. FAILED despues de CONFIRMED) no puede pisar un estado final.
    const { count } = await this.prisma.outboxEvent.updateMany({
      where: { id: msg.eventId, status: { in: ['PENDING', 'SENT'] } },
      data: msg.ok
        ? { status: 'CONFIRMED', confirmedAt: new Date(), lastError: null }
        : { status: 'FAILED', lastError: msg.error ?? 'error desconocido' },
    });
    // La NAS ya tiene el archivo: el temporal del servidor deja de hacer falta
    if (msg.ok && msg.destino === 'NAS') await this.borrarTemporal(msg.documentoId);

    this.logger.log(
      count
        ? `Resultado ${msg.destino} doc=${msg.documentoId} ok=${msg.ok}`
        : `Resultado ignorado (estado ya final o evento desconocido) evento=${msg.eventId}`,
    );
  }

  private async borrarTemporal(documentoId: string) {
    const doc = await this.prisma.documento.findUnique({
      where: { id: documentoId },
      select: { tempPath: true },
    });
    if (!doc) return;
    await unlink(doc.tempPath).catch((e) => {
      if (e?.code !== 'ENOENT') {
        this.logger.warn(`No se pudo borrar ${doc.tempPath}: ${e}`);
      }
    });
  }
}
