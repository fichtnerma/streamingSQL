import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { DataProducerModule } from './data-producer/data-producer.module';
import { PrismaModule } from './prisma/prisma.module';

@Module({
  imports: [DataProducerModule, PrismaModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
