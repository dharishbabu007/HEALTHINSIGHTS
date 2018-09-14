import { Component, OnInit } from '@angular/core';
import { UserSettingService } from './userSetting.service';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
import { Router } from '@angular/router';

import { GapsService } from '../../shared/services/gaps.service';
import { MessageService } from '../../shared/services/message.service';
import { UserSetting } from './userSetting.component';

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

     

      
      this.gapsService.getSecurityQuestions().subscribe((data: any) => {
        this.QuestionList = [];
        console.log(data)
        data.forEach(element => {
          this.QuestionList.push({label: element.question, value: element.question});
        });
      });
     }

  ngOnInit() {



  }

  submitPc(modelPc: UserSetting, isValid: boolean) {

       this.submitted = true;
      // call API to save
      // ...
      console.log( 'Model' + JSON.stringify(modelPc));
    this.UserSettingService.UserSettingSubmit(modelPc).subscribe( model => console.log('Succeessfully Updates UserSettings'));
    this.router.navigateByUrl('/dashboard');
  }

  
}
export interface UserSetting{
  securityQuestion: string;
}