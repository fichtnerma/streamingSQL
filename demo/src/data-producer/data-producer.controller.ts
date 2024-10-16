import { Controller, Post, Body, Get } from '@nestjs/common';
import { DataProducerService } from './data-producer.service';
import { CreateDataProducerDto } from './dto/create-data-producer.dto';

@Controller('data-producer')
export class DataProducerController {
  constructor(private readonly dataProducerService: DataProducerService) {}

  @Post()
  async generate(@Body() createDataProducerDto: CreateDataProducerDto) {
    await this.dataProducerService.clear();
    return await this.dataProducerService.generate(createDataProducerDto);
  }
}
