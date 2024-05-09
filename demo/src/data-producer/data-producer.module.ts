import { Module } from '@nestjs/common';
import { DataProducerService } from './data-producer.service';
import { DataProducerController } from './data-producer.controller';

@Module({
  controllers: [DataProducerController],
  providers: [DataProducerService],
})
export class DataProducerModule {}
