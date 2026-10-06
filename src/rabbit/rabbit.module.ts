import { RabbitMQModule } from '@golevelup/nestjs-rabbitmq';
import { Global, Module } from '@nestjs/common';

// Una sola conexion a RabbitMQ, visible (AmqpConnection) en todos los modulos
@Global()
@Module({
  imports: [
    RabbitMQModule.forRoot({
      uri: process.env.RABBITMQ_URI ?? 'amqp://admin:admin@localhost:5672',
      exchanges: [
        {
          name: 'new_message',
          type: 'topic',
          options: { durable: true },
        },
      ],
      // mensajes persistentes por defecto (sobreviven reinicio del broker)
      defaultPublishOptions: { persistent: true },
      connectionInitOptions: { wait: true, timeout: 60_000, reject: true },
    }),
  ],
  exports: [RabbitMQModule],
})
export class RabbitModule {}
