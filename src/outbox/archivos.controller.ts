import {
  Controller,
  Get,
  Headers,
  NotFoundException,
  Param,
  ParseUUIDPipe,
  Res,
  StreamableFile,
  UnauthorizedException,
} from '@nestjs/common';
import type { Response } from 'express';
import { timingSafeEqual } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { stat } from 'node:fs/promises';
import { PrismaService } from '../prisma/prisma.service';

/**
 * Entrega el archivo temporal de un documento al consumer de la NAS, que vive
 * en otra maquina y por eso no puede leer el disco del publisher.
 * Protegido con FILES_TOKEN (Authorization: Bearer <token>). Sin token
 * configurado el endpoint queda cerrado.
 */
@Controller('archivos')
export class ArchivosController {
  constructor(private readonly prisma: PrismaService) {}

  @Get(':documentoId')
  async descargar(
    @Param('documentoId', ParseUUIDPipe) documentoId: string,
    @Headers('authorization') authorization: string | undefined,
    @Res({ passthrough: true }) res: Response,
  ) {
    this.validarToken(authorization);

    const doc = await this.prisma.documento.findUnique({
      where: { id: documentoId },
      select: { tempPath: true, filename: true, mimeType: true },
    });
    const info = doc && (await stat(doc.tempPath).catch(() => null));
    if (!doc || !info) {
      // ya se borro (NAS confirmada) o nunca existio
      throw new NotFoundException('Archivo no disponible');
    }

    res.setHeader('Content-Length', info.size);
    return new StreamableFile(createReadStream(doc.tempPath), {
      type: doc.mimeType,
    });
  }

  private validarToken(authorization?: string) {
    const esperado = process.env.FILES_TOKEN;
    const recibido = authorization?.startsWith('Bearer ')
      ? authorization.slice('Bearer '.length)
      : '';
    const a = Buffer.from(recibido);
    const b = Buffer.from(esperado ?? '');
    if (!esperado || a.length !== b.length || !timingSafeEqual(a, b)) {
      throw new UnauthorizedException();
    }
  }
}
