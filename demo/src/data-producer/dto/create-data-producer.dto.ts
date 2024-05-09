import { IsInt, IsNumber, Max, Min } from 'class-validator';

export class CreateDataProducerDto {
  @IsNumber()
  @IsInt()
  changesPerSecond: number;

  @IsNumber()
  @IsInt()
  runTime: number;

  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  @Max(1)
  skew: number;
}
