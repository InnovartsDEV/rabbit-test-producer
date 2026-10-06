import { Module } from '@nestjs/common';
import { ArchivosController } from './archivos.controller';
import { OutboxController } from './outbox.controller';
import { OutboxRelayService } from './outbox-relay.service';
import { OutboxReintentosService } from './outbox-reintentos.service';
import { OutboxResultadosConsumer } from './outbox-resultados.consumer';
import { OutboxService } from './outbox.service';
import { TemporalesLimpiezaService } from './temporales-limpieza.service';

@Module({
  controllers: [OutboxController, ArchivosController],
  providers: [
    OutboxService,
    OutboxRelayService,
    OutboxReintentosService,
    OutboxResultadosConsumer,
    TemporalesLimpiezaService,
  ],
  exports: [OutboxService],
})
export class OutboxModule {}
