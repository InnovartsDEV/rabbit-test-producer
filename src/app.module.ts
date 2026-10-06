import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { OutboxModule } from './outbox/outbox.module';
import { PrismaModule } from './prisma/prisma.module';
import { RabbitModule } from './rabbit/rabbit.module';

@Module({
  imports: [RabbitModule, PrismaModule, OutboxModule],
  controllers: [AppController],
})
export class AppModule { }
