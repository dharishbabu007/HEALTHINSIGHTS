import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
import { Router } from '@angular/router';
import { DatePipe } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { MessageService } from '../../shared/services/message.service';
import { GapsService } from '../../shared/services/gaps.service';
import { PatScreen } from './pat-screen.component';
import { PatScreenService } from './pat-screen-service';


@Component({
  selector: 'app-pat-screen',
  templateUrl: './pat-screen.component.html',
  styleUrls: ['./pat-screen.component.scss'],
  providers: [GapsService]
})
export class PatComponent implements OnInit {
  memberID1: string;
  gaps: any =null;
  public myForm: FormGroup;
  populationList: any;
  measureList: any;
  codeTypeList: any;
  valueList: any;
  codesList: any;
  patientStatusList= [{label: "Numerator",value:"numerator"},{label: "Denominator",value:"denominator"},{label: "Exclusion",value:"exclusion"}];
  memberList: any;
  selectedMeasuresk: any;
  SelectedFile: File = null;
  fd: any;
  mrn: any;
  memberId: any;
  measureSk: any;
  patData: any;
  cols: any;
  firstData: any;
  populationRepositry:any
  measureRepositry:any;
  measureFromParam:any;
  selectedValueSet:any;
  selectedCodeType:any;
  plan:any;
  temp:any;
  constructor(private _fb: FormBuilder,
    private router: Router,
    private route: ActivatedRoute,
  private msgService: MessageService,
private GapsService: GapsService,
private PatScreenService:PatScreenService) {
  this.route.params.subscribe(params => {
    this.memberId = params['memberId'];
    this.measureSk = params['measureSK'];
    this.plan = params['plan'];
    });

    this.myForm = this._fb.group({
      population: [''],
      measure: [''],
      associatedPList: [''],
      patientStatus: [''],
      codeType: [''],
      valueSet: [''],
      codes:[''],
      reason: [''],
      bpSystolic:[''],
      bpDiastolic:[''],
      bmi:[''],
      patientDeathRate:[''],
      patientId:['']

    });
    
    this.GapsService.getPopulation().then((res:any)=>
    {
      this.populationList =[];
      this.populationRepositry = res;
      res.forEach(element =>{
      this.populationList.push({label: element.name,value:element.value})
    })
    //console.log(this.populationRepositry)
    let populationvalue =  this.populationRepositry.filter(element => element.value === this.plan);
   // console.log(populationvalue,this.plan)
    this.myForm.controls['population'].setValue(populationvalue[0].value);
  });
      this.GapsService.getPatMeasure().then((res:any)=>
        {
      this.measureList =[];
      this.measureRepositry = res;
      res.forEach(element =>{
        this.measureList.push({label: element.name,value:element.value})
      })
      this.measureFromParam =  this.measureRepositry.filter(element => element.value === this.measureSk);
      this.myForm.controls['measure'].setValue(this.measureFromParam[0].value);
    });
      this.GapsService.getPatCodeSet(this.measureSk).then((data:any)=>{
        this.valueList =[];
        this.codeTypeList =[];
        this.codesList =[];
        data.valueSet.forEach(element =>{
          this.valueList.push({label:element,value: element})
        })
      //   data.codeType.forEach(element =>{
      //     this.codeTypeList.push({label:element,value:element})
      //   })
      //   data.codes.forEach(element =>{
      //    this.codesList.push({label:element,value:element})
      //  })
     
      });
    }

  ngOnInit() {

      if(this.memberId){
         this.GapsService.getPatHistory(this.memberId,this.measureSk).subscribe((res: any)=>{
           if(res!=""){
            // console.log(res)
           this.mrn = res[0].mrn;
           this.patData =[];
           for(let i=1;i<res.length;i++){
            this.patData.push(res[i])
            }
           this.setMeasureInfo(res[0]);
          }
           this.loadMemberInfoOnStart(this.memberId);
         });
         this.cols=[
           {field: 'compliantFlag',header:'Compliant Flag'},
           {field: 'lobId',header:'Population'},
           {field: 'appointmentDate',header:'Appointment Date'},
           {field: 'gender',header:'Gender'},
           {field: 'dob',header:'Date of Birth'},
           {field: 'memberStatus',header:'Member Status'},
           {field: 'valueSet',header:'Value Set'},
           {field: 'codeType',header:'Code Type'},
           {field: 'uploadFilesList',header:'Attachment'},
         ];
 
       }
  }
  setMeasureInfo(data){
      this.GapsService.getPopulation().then((res:any)=>
      {
        this.populationRepositry = res;
        let populationvalue =  this.populationRepositry.filter(element => element.name === data.lobId);
        this.myForm.controls['population'].setValue(populationvalue[0].value);
      });
            this.myForm.controls['measure'].setValue(data.measureSk);
            this.myForm.controls['patientStatus'].setValue(data.memberStatus);
            this.myForm.controls['valueSet'].setValue(data.valueSet);
            this.temp={label:data.valueSet,value:data.valueSet};
            this.filterValueSet(this.temp);
            this.myForm.controls['codeType'].setValue(data.codeType);
            let temp1={label:data.codeType,value:data.codeType};
            this.filterCodeType(temp1);
            this.myForm.controls['codes'].setValue(data.codes);
            this.myForm.controls['reason'].setValue(data.reason);
            this.myForm.controls['patientId'].setValue(data.patientId);    
  }

formatDate(dateString) {
    if (dateString) {
      const datePipe = new DatePipe('en-US');
      return datePipe.transform(dateString, 'dd-MMM-yy');
    } else {
      return null;
    }
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

cancelPc() {
  this.myForm.reset();
  this.gaps =[];
  this.mrn=null;
}
myUploader(event){
this.SelectedFile= <File>event.target.files[0];
  this.fd = new FormData();
  this.fd.append('file', this.SelectedFile);
}
patientDateRate: boolean = false;
filterPatientStatus(event){
  if(event.value == "Exclusion"){
    this.patientDateRate = true;
  }
  else{
    this.patientDateRate = false;
  }

}
filterMeasure(event){
  this.selectedMeasuresk = event.value;
 this.GapsService.getPatCodeSet(event.value).then((data:any)=>{
   this.valueList =[];
   this.codeTypeList =[];
   this.codesList =[];
   data.valueSet.forEach(element =>{
     this.valueList.push({label:element,value: element})
   })
  //  data.codeType.forEach(element =>{
  //    this.codeTypeList.push({label:element,value:element})
  //  })
  //  data.codes.forEach(element =>{
  //   this.codesList.push({label:element,value:element})
  // })

 });
}
filterValueSet(event){
  if(this.measureSk){
    this.PatScreenService.getCodeType(this.measureSk,event.value).subscribe((res:any)=>{
      console.log(res)
      res.forEach(element =>{
        this.codeTypeList.push({label:element,value:element})
      })
    })
  }
  else{
    this.selectedValueSet = event.value;
    this.PatScreenService.getCodeType(this.selectedMeasuresk,this.selectedValueSet).subscribe((res:any)=>{
      console.log(res)
      res.forEach(element =>{
        this.codeTypeList.push({label:element,value:element})
      })
    })
  }
}
filterCodeType(event){
  if(this.measureSk){
    this.PatScreenService.getCodeList(this.measureSk,this.temp.value,event.value).subscribe((res:any)=>{
      res.forEach(element =>{
        this.codesList.push({label:element,value:element})
      })
    })

  }
  else{
    this.selectedCodeType = event.value;
    this.PatScreenService.getCodeList(this.selectedMeasuresk,this.selectedValueSet,this.selectedCodeType).subscribe((res:any)=>{
      res.forEach(element =>{
        this.codesList.push({label:element,value:element})
      })
    })
  }
}
search(event) {
  if(this.selectedMeasuresk){
  this.GapsService.getmrnList(this.selectedMeasuresk,event.query).then((data: any) => {
      this.memberList = [];
      data.forEach(element => {
          this.memberList.push(`${element.patId} ${element.name} [${element.patMrn}]`);
      });
  });
 }
}
loadMemberInfo(memberString) {
  const pattern = /\[(.*?)\]/g;
  const match = pattern.exec(memberString);
  if (match.length) {
    this.GapsService.getmrnList(this.selectedMeasuresk,match[1]).then((res:any)=>{
    //  console.log(res)
      this.memberID1 = res[0].patId;
      this.mrn = res[0].patMrn;
      this.GapsService.getGaps2(this.memberID1).subscribe((data: any) => {
        this.gaps =[];
        this.gaps = data;
    });
    })
     
  }
  this.GapsService.getPatHistory(this.memberID1,this.selectedMeasuresk).subscribe((res: any)=>{
  })
}
loadMemberInfoOnStart(memberId){
  this.myForm.controls['patientId'].setValue(memberId);    
    this.GapsService.getGaps2(this.memberId).subscribe((data: any) => {
      this.gaps =[];
      this.gaps = data;
      this.mrn = this.gaps.mrn;
  });
}

onSubmit() {
  if (this.myForm.valid) {
    this.submitPc(this.myForm.value, this.myForm.valid);
  } else {
    this.validateAllFormFields(this.myForm);
  }
}
type:any;
  submitPc(model, isValid: boolean) {
    if(this.gaps.compliantFlag == "No"){
      this.gaps.compliantFlag = "N"
    }
    else{
      this.gaps.compliantFlag = "Y"
    }
  //  console.log(this.gaps)
  this.GapsService.createPat(model,this.gaps,this.mrn).subscribe((res:any)=>{
    if (res.status === 'SUCCESS') {
      this.msgService.success('MIT Performed Successfully');
      this.type ="mit"; 
      this.PatScreenService.commonfileupload(this.type,this.fd).subscribe((res:any)=>{
        if(res.status === 'SUCCESS'){ this.msgService.success('File Upload Sucess');
        } else {
          this.msgService.error(res.message);
        }
      });
    } else {
      this.msgService.error(res.message);
    }
  });
  }
  downloadFile(event){
  //  console.log(event);
    const filetype = event.uploadFilesList[0].slice(-3);
   // console.log(filetype)
    if(filetype=="png"||filetype=="jpg"){
        this.PatScreenService.downloadimage(event.uploadFilesList[0]).subscribe((res:any)=> {
            this.saveimage(res)
       if(res){
           this.msgService.success('Download Successfully');
       }
       else {
          this.msgService.error(res.message);
        }
   })
    }
    else{
        this.PatScreenService.downloadfile(event.uploadFilesList[0]).subscribe((res:any)=> {
            this.savefile(res)
       if(res){
           this.msgService.success('Download Successfully');
       }
       else {
          this.msgService.error(res.message);
        }
   })
    }
}
savefile(data){
var blob = new Blob([(<any>data)], {type: 'text/csv'});
var url= window.URL.createObjectURL(blob);
var anchor = document.createElement("a");
anchor.download = "Mit.csv";
anchor.href = url;
anchor.click();
console.log();
}
saveimage(data){
var blob = new Blob([(<any>data)], {type: 'mime'});
var url= window.URL.createObjectURL(blob);
var anchor = document.createElement("a");
anchor.download = "mit.jpg";
anchor.href = url;
anchor.click();
console.log();
}

}

export interface PatScreen {

}

