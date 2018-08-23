import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ForgotPasswordRoutingModule } from './forgot-password-routing.module';
import { ForgotPasswordComponent } from './forgot-password.component';

@NgModule({
  imports: [
    CommonModule,
    ForgotPasswordRoutingModule
  ],
  declarations: [ForgotPasswordComponent]
})
export class ForgotPasswordModule { }
