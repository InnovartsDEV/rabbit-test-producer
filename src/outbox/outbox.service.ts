import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import { OutboxDestino, OutboxStatus, Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { estadoDocumento } from './estado-documento';
import type { EstadoDocumento } from './estado-documento';
import { RK_GUARDAR_BD, RK_GUARDAR_NAS } from './outbox.constants';

export interface NuevoDocumento {
  title?: string;
  author?: string;
  filename: string;
  mimeType: string;
  fileSize: number;
  tempPath: string;
  idempotencyKey?: string;
}

type ConteoEstados = Record<OutboxStatus, number>;

const estadosVacios = (): ConteoEstados => ({
  PENDING: 0,
  SENT: 0,
  CONFIRMED: 0,
  FAILED: 0,
});

@Injectable()
export class OutboxService {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Guarda N documentos y sus 2 eventos (NAS y BD) por documento en UNA
   * transaccion (todo o nada). No toca RabbitMQ: el relay publica despues.
   *
   * Idempotente: si un documento trae `idempotencyKey` ya registrada, no se
   * crea de nuevo y se devuelve el existente con `duplicado: true`.
   */
  async registrarDocumentos(docs: NuevoDocumento[], loteId = randomUUID()) {
    // Dos requests concurrentes con la misma key: el 2do choca con el unique
    // (P2002) y aborta su transaccion; al reintentar ya encuentra el existente.
    for (let intento = 0; ; intento++) {
      try {
        const documentos = await this.prisma.$transaction((tx) =>
          this.registrarEnTx(tx, docs, loteId),
        );
        return { loteId, documentos };
      } catch (error) {
        const choqueUnique = (error as { code?: string }).code === 'P2002';
        if (!choqueUnique || intento >= 2) throw error;
      }
    }
  }

  private async registrarEnTx(
    tx: Prisma.TransactionClient,
    docs: NuevoDocumento[],
    loteId: string,
  ) {
    const resultado: { id: string; filename: string; duplicado: boolean }[] = [];

    for (const { idempotencyKey, ...doc } of docs) {
      if (idempotencyKey) {
        const existente = await tx.documento.findUnique({
          where: { idempotencyKey },
        });
        if (existente) {
          resultado.push({
            id: existente.id,
            filename: existente.filename,
            duplicado: true,
          });
          continue;
        }
      }

      const documento = await tx.documento.create({
        data: { ...doc, idempotencyKey, loteId },
      });

      const base = {
        documentoId: documento.id,
        title: doc.title ?? null,
        author: doc.author ?? null,
        createdAt: documento.createdAt.toISOString(),
        filename: doc.filename,
        mimeType: doc.mimeType,
        fileSize: doc.fileSize,
      };

      await tx.outboxEvent.createMany({
        data: [
          {
            documentoId: documento.id,
            destino: OutboxDestino.NAS,
            routingKey: RK_GUARDAR_NAS,
            payload: base,
          },
          {
            documentoId: documento.id,
            destino: OutboxDestino.BD,
            routingKey: RK_GUARDAR_BD,
            payload: base,
          },
        ],
      });

      resultado.push({
        id: documento.id,
        filename: documento.filename,
        duplicado: false,
      });
    }

    return resultado;
  }

  /** Estado del lote: cuantos documentos y en que estado esta cada destino. */
  async lote(loteId: string) {
    const docs = await this.prisma.documento.findMany({
      where: { loteId },
      include: { eventos: true },
      orderBy: { createdAt: 'asc' },
    });

    const resumen: Record<OutboxDestino, ConteoEstados> = {
      NAS: estadosVacios(),
      BD: estadosVacios(),
    };
    for (const d of docs) {
      for (const e of d.eventos) resumen[e.destino][e.status]++;
    }

    const porEstado: Record<EstadoDocumento, number> = {
      COMPLETO: 0,
      PARCIAL: 0,
      EN_PROCESO: 0,
      REINTENTANDO: 0,
      ERROR: 0,
    };
    for (const d of docs) porEstado[estadoDocumento(d.eventos, d.reintentos)]++;

    return {
      loteId,
      documentos: docs.length,
      resumen,
      porEstado,
      // exito de cara al usuario: guardo en NAS, en BD o en ambos
      exitosos: porEstado.COMPLETO + porEstado.PARCIAL,
      // ya no queda nada en vuelo ni por reintentar
      terminado:
        docs.length > 0 &&
        porEstado.EN_PROCESO === 0 &&
        porEstado.REINTENTANDO === 0,
      completo:
        docs.length > 0 &&
        docs.every((d) => d.eventos.every((e) => e.status === 'CONFIRMED')),
    };
  }

  async stats() {
    const [documentos, grupos] = await Promise.all([
      this.prisma.documento.count(),
      this.prisma.outboxEvent.groupBy({
        by: ['destino', 'status'],
        _count: { _all: true },
      }),
    ]);

    const porDestino: Record<OutboxDestino, ConteoEstados> = {
      NAS: estadosVacios(),
      BD: estadosVacios(),
    };
    for (const g of grupos) {
      porDestino[g.destino][g.status] = g._count._all;
    }

    return {
      documentos,
      guardadosEnNas: porDestino.NAS.CONFIRMED,
      guardadosEnBd: porDestino.BD.CONFIRMED,
      porDestino,
    };
  }

  async documentos(limit = 50) {
    const docs = await this.prisma.documento.findMany({
      orderBy: { createdAt: 'desc' },
      take: limit,
      include: { eventos: true },
    });
    return docs.map((d) => this.vistaDocumento(d));
  }

  /** Un documento con su estado de cara al usuario; null si no existe. */
  async documento(id: string) {
    const doc = await this.prisma.documento.findUnique({
      where: { id },
      include: { eventos: true },
    });
    return doc ? this.vistaDocumento(doc) : null;
  }

  private vistaDocumento(d: {
    id: string;
    filename: string;
    createdAt: Date;
    reintentos: number;
    eventos: { destino: OutboxDestino; status: OutboxStatus; lastError: string | null }[];
  }) {
    const evento = (destino: OutboxDestino) =>
      d.eventos.find((e) => e.destino === destino);
    return {
      id: d.id,
      filename: d.filename,
      createdAt: d.createdAt,
      estado: estadoDocumento(d.eventos, d.reintentos),
      reintentos: d.reintentos,
      nas: { status: evento('NAS')?.status, error: evento('NAS')?.lastError },
      bd: { status: evento('BD')?.status, error: evento('BD')?.lastError },
    };
  }
}
