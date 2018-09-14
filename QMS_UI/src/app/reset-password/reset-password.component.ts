import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../router.animations';
import { Router } from '@angular/router';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { LoginService } from '../login/login.service';
import { MessageService } from '../shared/services/message.service';
import { AuthenticationService } from '../shared/services/authenticationservice';
import { first } from 'rxjs/operators';
import { User } from  '../shared/modules/user'
@Component({
    selector: 'app-reset-password',
    templateUrl: './reset-password.component.html',
    styleUrls: ['./reset-password.component.scss'],
    animations: [routerTransition()]
})
export class ResetPasswordComponent implements OnInit {
    public myForm: FormGroup;
    currentUser: User;
    user: string;
    public submitted: boolean;
    constructor(private _fb: FormBuilder,
        public router: Router,
        public LoginService: LoginService,
    public msgService :MessageService,
    public authenticService :AuthenticationService) {}
 
    ngOnInit() {
        this.myForm = this._fb.group({
            password: ['', [Validators.required]],
            confirmPassword: ['',[Validators.required]]
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

   
    OnSubmit(model: ResetPassword,isValid: boolean)
    {
        this.submitted = true;

        console.log(isValid);
        if(isValid){
      
      
              console.log("came here");
               this.submitted = true;
              console.log( 'Model' + JSON.stringify(model));
      
        var user =  JSON.parse(localStorage.getItem('currentUser'));
     
        console.log(user.loginId);
        console.log(JSON.stringify(model.password));

        this.authenticService.Reset(user.loginId,user.password,model.password,model.confirmPassword).subscribe((res:any) =>{

        if(res.status == 'SUCCESS'){
            this.msgService.success(res.message);
            this.router.navigateByUrl('/dashboard?fetch');
        }
        else{
            this.msgService.error(res.error);

        }
    
        });
    }
    else{
        this.validateAllFormFields(this.myForm);
    }

    }


    
}
export interface ResetPassword{
    password: string;
    confirmPassword: string;
}