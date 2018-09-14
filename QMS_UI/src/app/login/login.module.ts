import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { LoginRoutingModule } from './login-routing.module';
import { LoginComponent } from './login.component';
import { LoginService } from './login.service';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

@NgModule({
    imports: [CommonModule, LoginRoutingModule,
     FormsModule,
      ReactiveFormsModule],
    declarations: [LoginComponent],
    providers: [LoginService,
       ]
})
export class LoginModule {}
