import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { MAX_REINTENTOS, REINTENTO_BASE_MS } from './estado-documento';

const POLL_MS = 3000;

/**
 * Si NAS y BD fallaron los dos para un documento, vuelve a dejar sus eventos
 * en PENDING (el relay los republica con el mismo eventId; los consumers son
 * idempotentes). Espera exponencial: 5s, 10s, 20s. Si solo falla uno de los
 * dos NO se reintenta: el documento se considera exitoso (PARCIAL).
 */
@Injectable()
export class OutboxReintentosService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxReintentosService.name);
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(private readonly prisma: PrismaService) {}

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
      await this.reintentar();
    } catch (error) {
      this.logger.error('Fallo el ciclo de reintentos', error);
    } finally {
      this.running = false;
    }
  }

  private async reintentar() {
    const candidatos = await this.prisma.documento.findMany({
      where: {
        reintentos: { lt: MAX_REINTENTOS },
        eventos: { every: { status: 'FAILED' } },
      },
      include: { eventos: true },
      take: 50,
    });

    for (const doc of candidatos) {
      const espera = REINTENTO_BASE_MS * 2 ** doc.reintentos;
      const ultimoFallo = Math.max(...doc.eventos.map((e) => e.updatedAt.getTime()));
      if (Date.now() - ultimoFallo < espera) continue;

      const reiniciado = await this.prisma.$transaction(async (tx) => {
        // Guarda optimista: si otra instancia ya lo reintento, reintentos cambio
        const { count } = await tx.documento.updateMany({
          where: { id: doc.id, reintentos: doc.reintentos },
          data: { reintentos: { increment: 1 } },
        });
        if (count === 0) return false;

        await tx.outboxEvent.updateMany({
          where: { documentoId: doc.id, status: 'FAILED' },
          data: { status: 'PENDING', attempts: 0 },
        });
        return true;
      });

      if (reiniciado) {
        this.logger.warn(
          `Documento ${doc.id}: NAS y BD fallaron, reintento ${doc.reintentos + 1}/${MAX_REINTENTOS}`,
        );
      }
    }
  }
}
