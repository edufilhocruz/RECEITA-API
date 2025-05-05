import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { AuthModule } from './auth/auth.module';
import { ConsultasModule } from './consultas/consultas.module';

@Module({
  imports: [AuthModule, ConsultasModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
