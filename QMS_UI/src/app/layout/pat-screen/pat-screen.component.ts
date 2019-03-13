import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
import { Router } from '@angular/router';
import { DatePipe } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { MessageService } from '../../shared/services/message.service';
import { GapsService } from '../../shared/services/gaps.service';
import { PatScreen } from './pat-screen.component';


@Component({
  selector: 'app-pat-screen',
  templateUrl: './pat-screen.component.html',
  styleUrls: ['./pat-screen.component.scss'],
  providers: [GapsService]
})
export class PatComponent implements OnInit {
  memberID: string;
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
  constructor(private _fb: FormBuilder,
    private router: Router,
    private route: ActivatedRoute,
  private msgService: MessageService,
private GapsService: GapsService) {
  this.route.params.subscribe(params => {
    this.memberId = params['memberId'];
    this.measureSk = params['measureSK'];
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
      patientDeathRate:['']

    });
    }

  ngOnInit() {

    this.GapsService.getPopulation().then((res:any)=>
    {
      this.populationList =[];
     // console.log(res)
      res.forEach(element =>{
      this.populationList.push({label: element,value:element})
    })});
      this.GapsService.getPatMeasure().then((res:any)=>
        {
      this.measureList =[];
     // console.log(res)
      res.forEach(element =>{
        this.measureList.push({label: element.name,value:element.value})
      })});
      // this.GapsService.getAssociatedPatientList(measuresk,mrnid).subscribe((res:any)=>
      // {
      // });

      if(this.memberId){
         this.GapsService.getPatHistory(this.memberId,this.measureSk).subscribe((res: any)=>{
           this.patData =[];
           this.patData = res;
             //this.firstData = res[0];
           this.setMeasureInfo(res[0]);
            
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
 
         ];
 
       }

     
  }
  setMeasureInfo(data){
            this.myForm.controls['population'].setValue("hey");
            this.myForm.controls['measure'].setValue(data.measureSk);
            this.myForm.controls['patientStatus'].setValue(data.memberStatus);
            this.myForm.controls['codeType'].setValue(data.codeType);
            this.myForm.controls['codes'].setValue(data.codes);
            this.myForm.controls['valueSet'].setValue(data.valueSet);
            this.myForm.controls['reason'].setValue(data.reason);
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
}
myUploader(event){
 // console.log(event);
this.SelectedFile= <File>event.target.files[0];
  this.fd = new FormData();
  this.fd.append('file', this.SelectedFile);
}
patientDateRate: boolean = false;
filterPatientStatus(event){
 // console.log(event)
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
   data.codeType.forEach(element =>{
     this.codeTypeList.push({label:element,value:element})
   })
   data.codes.forEach(element =>{
    this.codesList.push({label:element,value:element})
  })

 });
}
search(event) {
  if(this.selectedMeasuresk){
 //console.log(this.selectedMeasuresk)
  this.GapsService.getmrnList(this.selectedMeasuresk,event.query).then((data: any) => {
      this.memberList = [];
      //console.log(this.memberList)
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
      //console.log(res)
      this.memberID = res[0].patId;
      this.mrn = res[0].patMrn;
     // console.log(this.memberID)
      this.GapsService.getGaps2(this.memberID).subscribe((data: any) => {
        this.gaps =[];
        this.gaps = data;
        //console.log(this.gaps)
    });
    })
     
  }
  this.GapsService.getPatHistory(this.memberID,this.selectedMeasuresk).subscribe((res: any)=>{
  //  console.log(res)
  })
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
 //console.log(model,this.gaps)
 
  this.GapsService.createPat(model,this.gaps,this.mrn).subscribe((res:any)=>{
 // console.log(model.patientStatus)
    if (res.status === 'SUCCESS') {
      this.msgService.success('Measure created Successfully');
     // this.router.navigateByUrl('/measureworklist?fetch');
    } else {
      this.msgService.error(res.message);
    }
  });
this.type ="mit";
  this.GapsService.commonfileupload(this.type,this.fd).subscribe((res:any)=>{
    if(res.status === 'SUCCESS'){ this.msgService.success('File Upload Sucess');
    } else {
      this.msgService.error(res.message);
    }
  })
   
    
  }

}

export interface PatScreen {

}

