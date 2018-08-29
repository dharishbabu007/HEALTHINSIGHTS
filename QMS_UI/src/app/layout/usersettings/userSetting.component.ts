import { Component, OnInit } from '@angular/core';
import { UserSettingService } from './userSetting.service';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
import { Router } from '@angular/router';
import { UserSetting } from './userSetting.component';

@Component({
  selector: 'app-userSetting',
  templateUrl: './userSetting.component.html',
  styleUrls: ['./userSetting.component.scss']
})
export class UserSettingComponent implements OnInit {

  public myForm: FormGroup;

  public submitted: boolean;

  constructor(private _fb: FormBuilder,
    private UserSettingService: UserSettingService,
    private router: Router) { }

  ngOnInit() {
      this.myForm = this._fb.group({
        firstName: [''],
        lastName: [''],
        securityQuestion: [''],
        securityAnswer: [''],
        phoneNumber: [''],
        EmailId: ['']
        
      });
  }

  submitPc(modelPc: UserSetting, isValid: boolean) {

       this.submitted = true;
      // call API to save
      // ...
      console.log( 'Model' + JSON.stringify(modelPc));
    this.UserSettingService.UserSettingSubmit(modelPc).subscribe( model => console.log('Succeessfully Created Program Creator'));
    this.router.navigateByUrl('/dashboard');
  }

  savePc(modelPC: UserSetting, isValid: boolean) {

    this.submitted = true;
   // call API to save
   // ...
   console.log('Model SavePC ' + JSON.stringify(modelPC));
 this.UserSettingService.UserSettingSubmit(modelPC).subscribe( model => console.log('Succeessfully Created Program Creator'));
}

}
export interface UserSetting{
  
}