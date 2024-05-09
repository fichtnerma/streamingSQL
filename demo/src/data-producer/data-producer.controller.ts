import { Controller, Post, Body } from '@nestjs/common';
import { DataProducerService } from './data-producer.service';
import { CreateDataProducerDto } from './dto/create-data-producer.dto';

@Controller('data-producer')
export class DataProducerController {
  constructor(private readonly dataProducerService: DataProducerService) {}

  @Post()
  create(@Body() createDataProducerDto: CreateDataProducerDto) {
    return this.dataProducerService.create(createDataProducerDto);
  }
}
