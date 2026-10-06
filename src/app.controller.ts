import {
  BadRequestException,
  Body,
  Controller,
  Headers,
  Post,
  UploadedFile,
  UploadedFiles,
  UseInterceptors,
} from '@nestjs/common';
import { AmqpConnection } from '@golevelup/nestjs-rabbitmq';
import { FileInterceptor, FilesInterceptor } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { unlink } from 'node:fs/promises';
import { extname, resolve } from 'node:path';
import { diskStorage } from 'multer';
import { EXCHANGE } from './outbox/outbox.constants';
import { OutboxService } from './outbox/outbox.service';

const MAX_ARCHIVOS_LOTE = 500;
const TMP_UPLOADS_DIR = resolve(process.cwd(), 'tmp-uploads');

const tmpStorage = diskStorage({
  destination: (_req, _file, cb) => {
    mkdirSync(TMP_UPLOADS_DIR, { recursive: true });
    cb(null, TMP_UPLOADS_DIR);
  },
  filename: (_req, file, cb) =>
    cb(null, `${randomUUID()}${extname(file.originalname)}`),
});

@Controller('message')
export class AppController {
  constructor(
    private readonly amqpConnection: AmqpConnection,
    private readonly outbox: OutboxService,
  ) { }

  @Post('send-1')
  async sendMessage(
    @Body() body: {
      message: string,
      status: boolean
    }) {

    await this.amqpConnection.publish(EXCHANGE, 'new.message1', {
      message: body.message,
      status: body.status,
      date: new Date().toISOString()
    })

    return {
      message: 'event in queue',
      data: body
    }
  }


  @Post('send-2')
  async sendMessage2(
    @Body() body: {
      message: string,
      status: boolean
    }) {

    await this.amqpConnection.publish(EXCHANGE, 'new.message2', {
      message: body.message,
      status: body.status,
      date: new Date().toISOString()
    })

    return {
      message: 'event in queue',
      data: body
    }
  }

  @Post('documento')
  @UseInterceptors(FileInterceptor('file', { storage: tmpStorage }))
  async guardarDocumento(
    @UploadedFile() file: Express.Multer.File,
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: { title?: string, author?: string }) {

    if (!file) {
      throw new BadRequestException("Falta el archivo (campo multipart 'file')");
    }

    // Outbox: documento + eventos NAS/BD en una sola transaccion.
    // El relay (OutboxRelayService) los publica a RabbitMQ despues.
    const { loteId, documentos } = await this.outbox.registrarDocumentos([{
      title: body?.title,
      author: body?.author,
      filename: file.originalname,
      mimeType: file.mimetype,
      fileSize: file.size,
      tempPath: resolve(file.path),
      idempotencyKey,
    }]);
    await this.limpiarTemporalesDuplicados([file], documentos);

    return {
      message: 'documento registrado, eventos guardar.nas y guardar.bd en outbox',
      data: { loteId, ...documentos[0] }
    }
  }

  /**
   * Lote: varios archivos (campo 'files') en una sola transaccion.
   * Header `Idempotency-Key` opcional: se deriva una clave por archivo
   * (`<key>:<indice>`), asi reenviar el mismo lote no crea documentos nuevos.
   */
  @Post('documentos')
  @UseInterceptors(FilesInterceptor('files', MAX_ARCHIVOS_LOTE, { storage: tmpStorage }))
  async guardarLote(
    @UploadedFiles() files: Express.Multer.File[],
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: { author?: string }) {

    if (!files?.length) {
      throw new BadRequestException("Faltan archivos (campo multipart 'files')");
    }

    const { loteId, documentos } = await this.outbox.registrarDocumentos(
      files.map((file, i) => ({
        author: body?.author,
        filename: file.originalname,
        mimeType: file.mimetype,
        fileSize: file.size,
        tempPath: resolve(file.path),
        idempotencyKey: idempotencyKey ? `${idempotencyKey}:${i}` : undefined,
      })),
    );
    await this.limpiarTemporalesDuplicados(files, documentos);

    return {
      message: `${documentos.length} documentos registrados en el outbox`,
      data: { loteId, documentos }
    }
  }

  // Un duplicado no genera eventos: su archivo temporal quedaria huerfano
  private async limpiarTemporalesDuplicados(
    files: Express.Multer.File[],
    documentos: { duplicado: boolean }[],
  ) {
    await Promise.all(
      files
        .filter((_, i) => documentos[i]?.duplicado)
        .map((f) => unlink(f.path).catch(() => undefined)),
    );
  }
}
