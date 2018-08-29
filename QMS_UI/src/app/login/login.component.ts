import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { Router } from '@angular/router';
import { routerTransition } from '../router.animations';

@Component({
    selector: 'app-login',
    templateUrl: './login.component.html',
    styleUrls: ['./login.component.scss'],
    animations: [routerTransition()]
})
export class LoginComponent implements OnInit {
  public myForm: FormGroup;

    constructor(private _fb: FormBuilder,public router: Router) {
    this.myForm = this._fb.group({
         username: ['', [Validators.required]],
      
         password: ['', [Validators.required]],
          
        });
     
    }
  
    ngOnInit() {
   
    }
   

   validateAllFormFields(formGroup: FormGroup) {
    Object.keys(formGroup.controls).forEach(field => {
    const control = formGroup.get(field);
    if (control instanceof FormControl) {
      control.markAsTouched({ onlySelf: true });
    } else if (control instanceof FormGroup) {
      this.validateAllFormFields(control);
    }
  });
}


    onLoggedin() { 
        if (this.myForm.valid) {
      this.router.navigateByUrl('/dashboard');
         } else {
      this.validateAllFormFields(this.myForm);
           }
    }
}
export interface login{
    username: string;
    password: string;
}