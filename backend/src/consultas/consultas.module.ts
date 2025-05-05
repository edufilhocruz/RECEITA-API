import { Module } from '@nestjs/common';
import { ConsultasController } from './consultas.controller';
import { ConsultasService } from './consultas.service';

@Module({
  controllers: [ConsultasController],
  providers: [ConsultasService]
})
export class ConsultasModule {}
