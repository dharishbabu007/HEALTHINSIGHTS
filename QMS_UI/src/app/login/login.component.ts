import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { Router,ActivatedRoute  } from '@angular/router';
import { routerTransition } from '../router.animations';
import { MessageService } from '../shared/services/message.service';
import { AuthenticationService } from '../shared/services/authenticationservice';
import { first } from 'rxjs/operators';
@Component({
    selector: 'app-login',
    templateUrl: './login.component.html',
    styleUrls: ['./login.component.scss'],
    animations: [routerTransition()] 
})
export class LoginComponent implements OnInit {
    public myForm: FormGroup;
    public submitted: boolean;
    returnUrl: string;

    constructor(private _fb: FormBuilder,
    public router: Router,
    public route: ActivatedRoute,
public msgService :MessageService,
public authenticService :AuthenticationService) {}
  
        ngOnInit() {
          console.log("came here2");
         this.myForm = this._fb.group({
             username: ['', [Validators.required]],
          
             password: ['', [Validators.required]],
              
            });

            this.authenticService.logout();
 
            // get return url from route parameters or default to '/'
            this.returnUrl = this.route.snapshot.queryParams['returnUrl'] || '/';


          
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
       



onSubmit(model: Login,isValid:boolean) {
  this.submitted = true;

  console.log(isValid);
  if(isValid){


        console.log("came here");
         this.submitted = true;
        console.log( 'Model' + JSON.stringify(model));


  this.authenticService.login(model.username, model.password)
      .pipe(first())
      .subscribe((res: any) => {
        console.log(res)
        console.log(res.loginId);
        console.log(res.resetPassword);
        if (res.loginId === model.username && res.resetPassword === 'Y') {

          this.msgService.success('Please reset your password');
          this.router.navigateByUrl('/reset-password');  
        } 
       else if(res.loginId === model.username && res.resetPassword != 'Y'){
         this.msgService.success('Login Succesfull')
        this.router.navigateByUrl('/dashboard');
          
        }
        else{
          this.msgService.error("please enter valid credentials");
        }

      } );
}
else{
  this.validateAllFormFields(this.myForm);
}
}

}

export interface Login{
    username: string;
    password: string;
}