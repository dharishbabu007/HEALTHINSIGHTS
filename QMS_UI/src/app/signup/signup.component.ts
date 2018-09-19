import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder, MinLengthValidator } from '@angular/forms';
import { Router,ActivatedRoute  } from '@angular/router';
import { routerTransition } from '../router.animations';
import { MessageService } from '../shared/services/message.service';
import { AuthenticationService } from '../shared/services/authenticationservice';
import { first, min } from 'rxjs/operators';
import { StringLiteral } from '../../../node_modules/typescript';
import { GapsService } from '../shared/services/gaps.service';
 
@Component({
    selector: 'app-signup',
    templateUrl: './signup.component.html',
    styleUrls: ['./signup.component.scss'],
    animations: [routerTransition()]
})
export class SignupComponent implements OnInit {
    public myForm: FormGroup;
    QuestionList: any;
    idList: any;
    model: any ={};
    tableRepository: any;
    public submitted: boolean;
    constructor(private _fb: FormBuilder,
        public router: Router,
        public route: ActivatedRoute,
    public msgService :MessageService, 
    public authenticService :AuthenticationService,
    private gapsService: GapsService,) {
    

      this.gapsService.getSecurityQuestions().subscribe((data: any) => {
        this.QuestionList = [];
        this.tableRepository = data;
        data.forEach(element => {
          this.QuestionList.push({label:element.question, value: element.question});
        });
      console.log(this.QuestionList)
      
      });

    }
    filterColumn(event){

      console.log(event.value)

     this.idList = this.tableRepository.filter(item => item.question === event.value);

      console.log(this.idList[0].id)
  }
    ngOnInit() {
        this.myForm = this._fb.group({

          name: ['', [Validators.required]],
          firstName: [''],
          lastName: [''],
          email: ['', [Validators.required]],

            securityQuestion: ['', [Validators.required]],

            securityAnswer: ['',  [Validators.required]],

            phNumber: ['', [Validators.required,  Validators.minLength(10)]],
         
            password: ['', [Validators.required, Validators.minLength(5)]],

            repeatPassword: ['', [Validators.required,  Validators.minLength(5)]]
             
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

    OnRegister(model: signup,isValid: boolean){

      this.submitted = true;
      
      console.log(isValid);
      if(isValid){
        if(model.password == model.repeatPassword){
          console.log(model.securityQuestion)
          model.securityQuestion = this.idList[0].id;
      this.authenticService.Register(model)
          .subscribe((res: any) => {
            console.log(res)
            if (res.status === 'SUCCESS') {
    
              this.msgService.success('Created');
              this.router.navigateByUrl('/login');  
            } 
        
            else{
              this.msgService.error("please enter valid credentials");
            }
    
          } );

        }
        else{
          this.msgService.error("Please enter same password and confirm password");
        }
      }
        else{
          this.validateAllFormFields(this.myForm);
        }
    

    }
}
export interface signup{
  name: string;
  firstName: string;
  lastName: string;
  email: string;
  password: string;
  repeatPassword: string;
  phNumber: number;
  securityQuestion: string;
  securityAnswer: string;

}