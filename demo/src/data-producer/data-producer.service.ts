import { Injectable } from '@nestjs/common';
import { CreateDataProducerDto } from './dto/create-data-producer.dto';
import { PrismaService } from 'src/prisma.service';

const MILISECONDS_IN_A_SECOND = 1000;
@Injectable()
export class DataProducerService {
  constructor(private prisma: PrismaService) {}
  async create(createDataProducerDto: CreateDataProducerDto) {
    const { changesPerSecond, runTime, skew } = createDataProducerDto;
    const intervalTime = MILISECONDS_IN_A_SECOND / changesPerSecond;
    const startTime = Date.now();
    const createdUsers = [];
    const interval = setInterval(async () => {
      if (Date.now() - startTime >= runTime * MILISECONDS_IN_A_SECOND) {
        clearInterval(interval);
      }
      if (Math.random() < skew) {
        await this.generateOrderData(
          createdUsers[Math.floor(Math.random() * createdUsers.length)],
        );
      } else {
        const user = await this.generateRandomUser();
        createdUsers.push(user.id);
      }
    }, intervalTime);
    return 'Data is being produced...';
  }

  private async generateRandomUser() {
    return this.prisma.user.create({
      data: {
        name: 'User',
        age: Math.floor(Math.random() * 100),
      },
    });
  }

  private async generateOrderData(userId: number) {
    return this.prisma.user.update({
      where: {
        id: userId,
      },
      data: {
        orders: {
          create: {
            total: Math.floor(Math.random() * 1000),
            nbrOfItems: Math.floor(Math.random() * 10),
          },
        },
      },
    });
  }
}
