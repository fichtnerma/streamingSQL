import { Injectable } from '@nestjs/common';
import { CreateDataProducerDto } from './dto/create-data-producer.dto';
import { PrismaService } from 'src/prisma/prisma.service';
import { NAME_LIST } from './data';
import { User } from '@prisma/client';

const MILISECONDS_IN_A_SECOND = 1000;

const UPDATE_PROBABILITY = 0.25;

@Injectable()
export class DataProducerService {
  constructor(private prisma: PrismaService) {}

  async clear() {
    await this.prisma.user.deleteMany();
    await this.prisma.order.deleteMany();
    return 'Data cleared';
  }

  // function to generate data inserts, updates and deletes
  // async generate(createDataProducerDto: CreateDataProducerDto) {
  //   const { changesPerSecond, runTime, skew, allowedOperations } =
  //     createDataProducerDto;
  //   let delay = MILISECONDS_IN_A_SECOND / changesPerSecond;
  //   let updates = 0;
  //   let deletes = 0;
  //   let inserts = 0;
  //   const startTime = Date.now();
  //   const endTime = startTime + runTime * MILISECONDS_IN_A_SECOND;
  //   while (Date.now() < endTime) {
  //     const startTime = Date.now();
  //     const operation = this.generateOperation(allowedOperations);
  //     const skewRandom = Math.random();
  //     const skewResult = skewRandom < skew ? 'order' : 'user';

  //     if (operation === 'delete') {
  //       this.delete(skewResult);
  //       deletes++;
  //     } else if (operation === 'update') {
  //       this.update(skewResult);
  //       updates++;
  //     } else {
  //       this.insert(skewResult);
  //       inserts++;
  //     }
  //     if (changesPerSecond) {
  //       await new Promise((resolve) =>
  //         setTimeout(resolve, delay - (Date.now() - startTime)),
  //       );
  //     }
  //   }
  //   return `Data generation completed: ${inserts} inserts, ${updates} updates, ${deletes} deletes`;
  // }
  async generate(createDataProducerDto: CreateDataProducerDto) {
    const { changesPerSecond, runTime, skew, allowedOperations } =
      createDataProducerDto;
    const records = [];
    for (let i = 0; i < runTime; i++) {
      records.push(
        this.generateRecords(changesPerSecond, skew, changesPerSecond * i + 1),
      );
    }
    let insertsUser = 0;
    let insertsOrder = 0;
    for (let i = 0; i < runTime; i++) {
      let recordsPerSec = records[i];
      this.prisma.$transaction([
        this.prisma.user.createMany({ data: recordsPerSec.userRecords }),
        this.prisma.order.createMany({ data: recordsPerSec.orderRecords }),
      ]);
      insertsUser += recordsPerSec.userRecords.length;
      insertsOrder += recordsPerSec.orderRecords.length;
      await new Promise((resolve) =>
        setTimeout(resolve, MILISECONDS_IN_A_SECOND),
      );
    }
    return `Data generation: ${insertsUser} User inserts, ${insertsOrder} Order inserts will happen over ${runTime} seconds`;
  }

  generateUser(id) {
    return {
      id: id,
      name: this.generateName(),
      age: Math.floor(Math.random() * 100),
    };
  }
  generateRecords(amount: number, skew: number, startIndex: number) {
    const userRecords = [];
    const orderRecords = [];
    const leftAmount = Math.max(Math.floor(amount * (1 - skew)), 1);
    const rightAmount = amount - leftAmount;
    for (let i = 0; i < leftAmount; i++) {
      userRecords.push(this.generateUser(i + startIndex));
    }
    for (let i = 0; i < rightAmount; i++) {
      orderRecords.push(
        this.generateOrder(
          i + startIndex,
          userRecords[Math.floor(Math.random() * userRecords.length)].id,
        ),
      );
    }
    return { userRecords, orderRecords };
  }
  generateOrder(id, buyerId) {
    return {
      id: id,
      buyerId: buyerId,
      nbrOfItems: Math.floor(Math.random() * 10),
      total: Math.floor(Math.random() * 1000),
    };
  }

  async insert(table: 'user' | 'order') {
    if (table === 'order') {
      const randomUser: User[] = await this.prisma
        .$queryRaw`SELECT * FROM "User" ORDER BY RANDOM() LIMIT 1;`;

      if (randomUser.length > 1) {
        const order = await this.prisma.order.create({
          data: {
            buyerId: randomUser[0].id,
            nbrOfItems: Math.floor(Math.random() * 10),
            total: Math.floor(Math.random() * 1000),
          },
        });
      } else {
        const name = this.generateName();
        const user = await this.prisma.user.create({
          data: {
            name,
            age: Math.floor(Math.random() * 100),
          },
        });
      }
    } else {
      const name = this.generateName();
      const user = await this.prisma.user.create({
        data: {
          name,
          age: Math.floor(Math.random() * 100),
        },
      });
    }
  }

  async update(table: 'user' | 'order') {
    if (table === 'order') {
      const randomOrder: User = await this.prisma
        .$queryRaw`SELECT * FROM "Order" ORDER BY RANDOM() LIMIT 1;`[0];
      if (randomOrder == undefined) {
        return;
      }
      await this.prisma.order.update({
        where: { id: randomOrder.id },
        data: {
          total: Math.floor(Math.random() * 1000),
          nbrOfItems: Math.floor(Math.random() * 10),
        },
      });
      return;
    } else {
      const randomUser: User = await this.prisma
        .$queryRaw`SELECT * FROM "User" ORDER BY RANDOM() LIMIT 1;`[0];
      if (randomUser == undefined) {
        return;
      }
      const name = this.generateName();
      await this.prisma.user.update({
        where: { id: randomUser.id },
        data: { name, age: Math.floor(Math.random() * 100) },
      });
    }
  }

  async delete(table: 'user' | 'order') {
    if (table === 'order') {
      const randomOrder: User = await this.prisma
        .$queryRaw`SELECT * FROM "Order" ORDER BY RANDOM() LIMIT 1;`[0];
      if (randomOrder == undefined) {
        return;
      }
      await this.prisma.order.delete({
        where: { id: randomOrder.id },
      });
      return;
    } else {
      const randomUser: User = await this.prisma
        .$queryRaw`SELECT * FROM public."User" ORDER BY RANDOM() LIMIT 1;`[0];
      if (randomUser == undefined) {
        return;
      }
      await this.prisma.user.delete({
        where: { id: randomUser.id },
      });
    }
  }

  generateOperation = (allowedOperations: {
    create: true;
    update: boolean;
    delete: boolean;
  }) => {
    const random = Math.random();
    if (allowedOperations.delete && random < UPDATE_PROBABILITY) {
      return 'delete';
    } else if (
      allowedOperations.update &&
      random >= UPDATE_PROBABILITY &&
      random < UPDATE_PROBABILITY * 2
    ) {
      return 'update';
    } else {
      return 'insert';
    }
  };

  generateName = () => {
    return NAME_LIST[Math.floor(Math.random() * NAME_LIST.length)];
  };
}
