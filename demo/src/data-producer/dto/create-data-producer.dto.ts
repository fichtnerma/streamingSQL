import { Optional } from '@nestjs/common';
import { IsInt, IsNumber, Max, Min } from 'class-validator';

export class CreateDataProducerDto {
  @IsNumber()
  @IsInt()
  changesPerSecond: number = 50;

  @IsNumber()
  @IsInt()
  runTime: number;

  @IsNumber({ maxDecimalPlaces: 4 })
  @Min(0)
  @Max(1)
  skew: number;

  @Optional()
  allowedOperations: {
    create: true;
    update: boolean;
    delete: boolean;
  };
}
