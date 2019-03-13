import { Component, OnInit } from '@angular/core';
import { GapsService } from '../../shared/services/gaps.service';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray} from '@angular/forms';
import { Router } from '@angular/router';

import { ActivatedRoute } from '@angular/router';
import { MessageService } from '../../shared/services/message.service';
import {TreeNode} from 'primeng/api';

@Component({ 
    selector: 'app-tables',
    templateUrl: './create-role.component.html', 
     styleUrls: ['./create-role.component.scss'],
    providers: [GapsService]
})
export class CreateRoleComponent implements OnInit {
    public myForm: FormGroup;
    measureId: string;
    roleList: any;
    Repositry: any;
    pageList: any;
    pageListRepositry: any;
    indi: any;
    i:any;
    k: any;
    selectWrite: boolean;
    readvalue: any;
    writevalue:any;
    downloadvalue: any;
    arr: Array<Object>;
    selectedPages: any;
    constructor(private _fb: FormBuilder,
      private GapsService: GapsService,
      private router: Router,
      private route: ActivatedRoute,
    private msgService: MessageService,) {
      this.route.params.subscribe(params => {
              this.measureId = params['measureId'];
          }); }
  
    ngOnInit() {
        this.myForm = this._fb.group({
            RoleName: [''],
            read: [''],
            roleCategorys: this._fb.array([
                this.initRoleCategorys(),
            ])
             
          });
        this.GapsService.getRoleList().subscribe((data: any) => {
            this.roleList =[];
            this.Repositry = data;
            data.forEach(item => {
            this.roleList.push({label: item.name, value: item.name});
            });
        });
        this.GapsService.getPageList().subscribe((data : any) =>{
            this.pageList =[];
            this.pageListRepositry = data;
            data.forEach(item => {
            this.pageList.push({label: item.name, value: item.name});
            });
        });

        //(<FormArray>this.myForm.controls['roleCategorys']).controls[0]['controls']['download'].disable();

     
        
    }

    get formData() { return <FormArray>this.myForm.get('roleCategorys'); }

  addCategory(j) {
      const control = <FormArray>this.myForm.controls['roleCategorys'];
      let newGroup = this._fb.group({
        parentPage: [''],
            read: [''],
            write: [''],
            download: ['']
    });
      control.push(newGroup);
    //  console.log(j);
     //(<FormArray>this.myForm.controls['roleCategorys']).controls[j]['controls']['write'].disable();
     //(<FormArray>this.myForm.controls['roleCategorys']).controls[j]['controls']['download'].disable();
     this.pageList 
  }

  removeCategory(i: number) {
      const control = <FormArray>this.myForm.controls['roleCategorys'];
      control.removeAt(i);
  }


    initRoleCategorys(){
        return this._fb.group({
            parentPage: [''],
            read: [''],
            write: [''],
            download: ['']
          }); 
    }

    checkedRead(event,i){
       // console.log(event)
        if(event == true){
         //  (<FormArray>this.myForm.controls['roleCategorys']).controls[i]['controls']['write'].enable();
         //  (<FormArray>this.myForm.controls['roleCategorys']).controls[i]['controls']['download'].enable();
        }
        else{
         //   (<FormArray>this.myForm.controls['roleCategorys']).controls[i]['controls']['write'].disable();
         //  (<FormArray>this.myForm.controls['roleCategorys']).controls[i]['controls']['download'].disable();
        }
    }
    pagelistChange(event,i){
        //console.log(event,'',i);
        (<FormArray>this.myForm.controls['roleCategorys']).controls[i]['controls']['read'].patchValue(true);
        //(<FormArray>this.myForm.controls['roleCategorys']).controls[i+1]['controls']['parentPage'].patchValue(true);
    }
    filterColumnRole(event){

      if((<FormArray>this.myForm.controls['roleCategorys']).length >1){
        this.myForm.controls['roleCategorys'].reset();
          let araylenght = (<FormArray>this.myForm.controls['roleCategorys']).length;
   
          for(this.i=0; this.i<=araylenght+1 ;  this.i++){
            const control = <FormArray>this.myForm.controls['roleCategorys'];
            //control.removeAt(this.i);
            this.removeCategory(this.i) 
          }
         this.removeCategory(1)
      }

        const roleId1 = this.Repositry.filter(item => item.name === event.value);
        this.pageListRepositry;
        this.GapsService.getRoleData(roleId1[0].value).subscribe((data: any) => {

                    this.k = data.screenPermissions.length;
                      // console.log(data)
                    //console.log(data.screenPermissions)
                     if(this.k>0){
                        
                    for(this.i= 0; this.i<this.k; this.i++){
               
                      let pagename = this.pageListRepositry.filter(item => item.value == data.screenPermissions[this.i].screenId);
                
                    (<FormArray>this.myForm.controls['roleCategorys']).controls[this.i]['controls']['parentPage'].patchValue(pagename[0].name);
                    if(data.screenPermissions[this.i].write == "Y"){
                    (<FormArray>this.myForm.controls['roleCategorys']).controls[this.i]['controls']['write'].patchValue(true);}
                    if(data.screenPermissions[this.i].read == "Y"){
                        (<FormArray>this.myForm.controls['roleCategorys']).controls[this.i]['controls']['read'].patchValue(true);}
                        if(data.screenPermissions[this.i].download == "Y"){
                            (<FormArray>this.myForm.controls['roleCategorys']).controls[this.i]['controls']['download'].patchValue(true);}
                    this.addCategory(this.i) 
                        }
                        this.removeCategory(this.k) 
                }
                else{
                        return false
                }
                //this.pageList.push({label: item.name, value: item.name});
    
        });
    
    }

    onSubmit(model,valid: boolean){      

   let roleId = this.Repositry.filter(item=> item.name === model.RoleName);

   for(this.i=0; this.i<model.roleCategorys.length; this.i++){
     //console.log(model.roleCategorys[this.i].parentPage)
     if(model.roleCategorys[this.i].parentPage != ""){
      //console.log(model.roleCategorys[this.i].parentPage)
       let screenid = this.pageListRepositry.filter(item => item.name === model.roleCategorys[this.i].parentPage);

       if(model.roleCategorys[this.i].read == "read" || model.roleCategorys[this.i].read == true)
       {
       // console.log("came here")
           this.readvalue = "Y"
       }/*
       else if(model.roleCategorys[this.i].read[0] == "read"){
        console.log("came here also")
        this.readvalue = "Y"
       }*/
       else{
        this.readvalue = "N"  
       }

       if(model.roleCategorys[this.i].write == "write" || model.roleCategorys[this.i].write == true)
       {
       // console.log("came here w" )
           this.writevalue = "Y"
       }/*
       else if(model.roleCategorys[this.i].write[0] == "write")
       {
           this.writevalue = "Y"
       }*/
       else{
        this.writevalue = "N" 
       }

       
       if(model.roleCategorys[this.i].download == "download" || model.roleCategorys[this.i].download == true)
       {
        //console.log("came here d")
           this.downloadvalue = "Y"
       }/*
       else if(model.roleCategorys[this.i].download[0] == "download")
       {
           this.downloadvalue = "Y"
       }*/
       else{
        this.downloadvalue = "N" 
       }
        //  console.log(screenid)
       this.arr =[{
         screenid : screenid[0].value,
        read: this.readvalue,
        write: this.writevalue,
       download: this.downloadvalue
      }]
    }
  console.log(this.arr);
    this.GapsService.createRole(roleId[0].value,this.arr).subscribe( (res: any) => {
        if (res.status === 'SUCCESS') {
          this.msgService.success('Role Pages Mapping Successfully');
          this.myForm.reset();
        } else {
          this.msgService.error(res.message);
        }
        });
   }


    }
    check(event){
        console.log(event)
    }   

}