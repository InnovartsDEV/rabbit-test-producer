import {
  Controller,
  Get,
  NotFoundException,
  Param,
  ParseUUIDPipe,
  Res,
} from '@nestjs/common';
import type { Response } from 'express';
import { httpStatusDe } from './estado-documento';
import { OutboxService } from './outbox.service';

@Controller('outbox')
export class OutboxController {
  constructor(private readonly outbox: OutboxService) {}

  @Get('stats')
  stats() {
    return this.outbox.stats();
  }

  @Get('lotes/:loteId')
  lote(@Param('loteId', ParseUUIDPipe) loteId: string) {
    return this.outbox.lote(loteId);
  }

  @Get('documentos')
  documentos() {
    return this.outbox.documentos();
  }

  /**
   * Estado de un documento para el usuario:
   *  200 COMPLETO o PARCIAL (si falla NAS o BD, pero no los dos, es exito)
   *  202 EN_PROCESO o REINTENTANDO (fallaron los dos, se esta reintentando)
   *  502 ERROR (fallaron los dos y se agotaron los reintentos)
   */
  @Get('documentos/:id')
  async documento(
    @Param('id', ParseUUIDPipe) id: string,
    @Res({ passthrough: true }) res: Response,
  ) {
    const doc = await this.outbox.documento(id);
    if (!doc) throw new NotFoundException('Documento no encontrado');
    res.status(httpStatusDe(doc.estado));
    return doc;
  }
}
