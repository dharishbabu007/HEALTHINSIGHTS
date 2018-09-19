import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../router.animations';
import { Router } from '@angular/router';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { ForgotService } from './forgot-password.service';
import { MessageService } from '../shared/services/message.service';

@Component({
    selector: 'app-forgot-password',
    templateUrl: './forgot-password.component.html',
    styleUrls: ['./forgot-password.component.scss'],
    animations: [routerTransition()]
})
export class ForgotPasswordComponent implements OnInit {

    public myForm: FormGroup;
    public submitted: boolean;
    constructor(private _fb: FormBuilder,
        public router: Router,
        public ForgotService: ForgotService,
    public msgService :MessageService) {}

    ngOnInit() {

        console.log("came here2");
        this.myForm = this._fb.group({
            
         
            emailId: ['', [Validators.required]],
             
           });

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
    onSubmit(model: Forgot, isValid: boolean) { 

           

        console.log(isValid);
        if(isValid){


              console.log("came here");
               this.submitted = true;
              console.log( 'Model' + JSON.stringify(model));


            this.ForgotService.Forgot(model).subscribe( (res: any) => {

                console.log(res)
                console.log(res.loginId)
                if (res.status === "SUCCESS") {

                  this.msgService.success(res.message);
                  this.router.navigateByUrl('/login');
                 
                } else {
                  this.msgService.error(res.message);
                }
              } );
            
        }
        else{
            this.validateAllFormFields(this.myForm);
          }
      
}



}
export interface Forgot{
    emailId: string;
}