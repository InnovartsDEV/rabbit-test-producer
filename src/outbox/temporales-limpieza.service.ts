import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { readdir, stat, unlink } from 'node:fs/promises';
import { join, resolve } from 'node:path';

const CADA_MS = 60 * 60 * 1000; // 1 hora
const RETENCION_DIAS = Number(process.env.TEMP_RETENTION_DAYS ?? 7);

/**
 * Los temporales de documentos que la NAS confirmo se borran al instante
 * (OutboxResultadosConsumer). Esto recoge los huerfanos: documentos cuya NAS
 * nunca confirmo, pasado el tiempo de retencion.
 */
@Injectable()
export class TemporalesLimpiezaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TemporalesLimpiezaService.name);
  private readonly dir = resolve(process.cwd(), 'tmp-uploads');
  private timer?: NodeJS.Timeout;

  onModuleInit() {
    this.timer = setInterval(() => void this.limpiar(), CADA_MS);
  }

  onModuleDestroy() {
    clearInterval(this.timer);
  }

  async limpiar() {
    const limite = Date.now() - RETENCION_DIAS * 24 * 60 * 60 * 1000;
    const archivos = await readdir(this.dir).catch(() => [] as string[]);
    let borrados = 0;
    for (const nombre of archivos) {
      const ruta = join(this.dir, nombre);
      const info = await stat(ruta).catch(() => null);
      if (info?.isFile() && info.mtimeMs < limite) {
        await unlink(ruta).then(() => borrados++).catch(() => undefined);
      }
    }
    if (borrados) this.logger.log(`Temporales antiguos borrados: ${borrados}`);
  }
}
