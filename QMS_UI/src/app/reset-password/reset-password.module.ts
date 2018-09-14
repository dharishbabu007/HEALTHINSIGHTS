import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ResetPasswordRoutingModule } from './reset-password-routing.module';
import { ResetPasswordComponent } from './reset-password.component';

import { FormsModule, ReactiveFormsModule } from '@angular/forms';
@NgModule({
  imports: [
    CommonModule,
    ResetPasswordRoutingModule,
    FormsModule,
    ReactiveFormsModule
  ],
  declarations: [ResetPasswordComponent]
})
export class ResetPasswordModule { } 
