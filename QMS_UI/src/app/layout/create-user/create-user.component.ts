import { Component, OnInit } from '@angular/core';
import { GapsService } from '../../shared/services/gaps.service';
import { FormGroup, FormControl, Validators, FormBuilder} from '@angular/forms';
import { Router } from '@angular/router';

import { ActivatedRoute } from '@angular/router';
import { MessageService } from '../../shared/services/message.service';
import { isTemplateExpression } from '../../../../node_modules/typescript';


@Component({
  selector: 'app-create-user',
  templateUrl: './create-user.component.html',
  styleUrls: ['./create-user.component.scss']
})
export class CreateUserComponent implements OnInit {


  users: string[] = ['user1','user2','user3','user4','user5','user9','user6','user1','user1','user1'];

    filteredusers: any[];
  public myForm: FormGroup;
  roleList: any[];
statusList:any;
  UserList: any;
  Repositry: any;
  userDataRepository: any;
  roleListRepositry:any;
  values: any;
  constructor(private _fb: FormBuilder,
    private GapsService: GapsService,
    private router: Router,
    private route: ActivatedRoute,
  private msgService: MessageService) { }
  ngOnInit() {
    this.myForm = this._fb.group({
      user:[''],
        roleName: ['', Validators.required],
        status: ['', Validators.required]

      });
      this.GapsService.getUserList().subscribe((data: any) => {
        this.UserList =[];
        this.Repositry = data;
        data.forEach(item => {
          this.UserList.push({label: item.name, value: item.value});
        });
        //console.log("came here")
        });
        this.GapsService.getRoleList().subscribe((data: any) => {
          this.roleList =[];
          this.roleListRepositry = data;
          data.forEach(item => {
            this.roleList.push({label: item.name, value: item.name});
          });
      });
      this.GapsService.getStatusList().subscribe((data: any) => {
        this.statusList =[];
        data.forEach(item => {
          this.statusList.push({label: item, value: item});
        });
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
onSubmit(model,isValid){
  if (this.myForm.valid) {
    
  
 //console.log(model);
           let userId = this.Repositry.filter(item => item.name === model.user);
           let roleId = this.roleListRepositry.filter(item => item.name === model.roleName);
           console.log(userId[0].value,roleId[0].value,model.status)
           this.GapsService.UserMappingSubmit(userId[0].value,roleId[0].value,model.status).subscribe( (res: any) => {
            if (res.status === 'SUCCESS') {
              this.msgService.success('details saved Successfully');
              this.myForm.reset();
            } else {
              this.msgService.error(res.message);
            }
          } );
        } else {
          this.validateAllFormFields(this.myForm);
        }
        
}


filteredUsers(event) {
  this.filteredusers = [];
    for(let i = 0; i <  this.Repositry.length; i++) {
      let user =  this.Repositry[i];
    if(user.name.toLowerCase().indexOf(event.query.toLowerCase()) == 0) {
       this.filteredusers.push(user);    
    }
  
  }  
}
getUserValues(event){
  let user = this.Repositry.filter(item => item.name === event.name);
  this.myForm.controls['user'].setValue(event.name);
  this.GapsService.getUserData(user[0].value).subscribe((data: any) => {
       this.userDataRepository = data;
       let roleId = this.roleListRepositry.filter(item => item.value === this.userDataRepository.roleId);
       this.myForm.controls['roleName'].setValue(roleId[0].name);
       this.myForm.controls['status'].setValue(this.userDataRepository.status);
     //  console.log(this.userDataRepository.status)
      // if(this.userDataRepository.status == "Verified"){
       //  console.log("came")
       //  this.statusList = this.statusList.filter(item=>item.label !="Verified");
     ///  }
      
});

}
onChangeList(event){
if( event == "Active"){
 this.statusList = this.statusList.filter(item => item.label == "Inactive");
}
//console.log(this.statusList)
//console.log(this.myForm.controls['status'].value)
if(this.myForm.controls['status'].value == "New"){
 this.statusList = this.statusList.filter(item => item.label !="New");
}    
}

}
