import { Component, OnInit } from '@angular/core';
import { UserSettingService } from './userSetting.service';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
import { Router } from '@angular/router';

import { GapsService } from '../../shared/services/gaps.service';
import { MessageService } from '../../shared/services/message.service';
import { UserSetting } from './userSetting.component';
import { isTemplateExpression } from 'typescript';

@Component({
  selector: 'app-userSetting',
  templateUrl: './userSetting.component.html',
  styleUrls: ['./userSetting.component.scss'],
    providers: [GapsService]
})
export class UserSettingComponent implements OnInit {

  public myForm: FormGroup;

  public submitted: boolean;

  QuestionList: any;
  Queid: any;
  Question: any;
  actualQuestion:string = "any";
  tableRepository: any;
  constructor(private _fb: FormBuilder,
    private UserSettingService: UserSettingService,
    private router: Router,
     private gapsService: GapsService,
    private msgService: MessageService,) {
      
      this.myForm = this._fb.group({
        firstName: [''],
        lastName: [''],
        securityQuestion: [''],
        securityAnswer: [''],
        phoneNumber: [''],
        EmailId: ['']
        
      });

      

    
     }
   
  ngOnInit() {
    var user =  JSON.parse(localStorage.getItem('currentUser'));

    this.Queid = user.securityQuestion;
     

    
    this.gapsService.getSecurityQuestions().subscribe((data: any) => {
      this.QuestionList = [];
      this.Question = [];
      this.tableRepository = data;
      data.forEach(element => {
        this.QuestionList.push({label: element.question, value: element.question});
      });
      
        this.Question = this.tableRepository.filter(item => item.id == this.Queid);
     this.actualQuestion =  this.Question[0].question;
     this.myForm.controls['securityQuestion'].setValue(this.actualQuestion);
     console.log(this.Queid)
 
     
    });

   
    this.myForm.controls['firstName'].setValue(user.firstName);
    this.myForm.controls['lastName'].setValue(user.lastName);
    
    this.myForm.controls['securityAnswer'].setValue(user.securityAnswer);
    this.myForm.controls['phoneNumber'].setValue(user.phoneNumber);
    this.myForm.controls['EmailId'].setValue(user.email);



  }

  getID(event){
   
  
}
  submitPc(modelPc: UserSetting, isValid: boolean) {
    var user =  JSON.parse(localStorage.getItem('currentUser'));
    this.Queid =  [];
    this.Queid = this.tableRepository.filter(item => item.question === modelPc.securityQuestion );
    modelPc.securityQuestion = this.Queid[0].id;

       this.submitted = true;
      // call API to save
      // ...
      console.log( 'Model' + user.loginId);
    this.UserSettingService.UserSettingSubmit(modelPc,user.loginId).subscribe( model => this.msgService.success('Successfully Updated UserSettings'));

  }

  
}
export interface UserSetting{
  firstName: string;
  lastName: string;
  loginId: string;
  securityAnswer:string;
  phoneNumber:string;
  EmailId: string;
  
  securityQuestion: string;
}