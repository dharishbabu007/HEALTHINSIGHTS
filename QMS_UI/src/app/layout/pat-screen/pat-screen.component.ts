import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
import { Router } from '@angular/router';
import { DatePipe } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { MessageService } from '../../shared/services/message.service';


@Component({
  selector: 'app-pat-screen',
  templateUrl: './pat-screen.component.html',
  styleUrls: ['./pat-screen.component.scss']
})
export class PatComponent implements OnInit {

  public myForm: FormGroup;
  populationList: any;
  caregapList: any;
  codeTypeList: any;
  valueList: any;
  codesList: any;
  patientStatusList: any;
  memberList: any;
  constructor(private _fb: FormBuilder,
    private router: Router,
    private route: ActivatedRoute,
  private msgService: MessageService) {
    }

  ngOnInit() {
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

formatDate(dateString) {
    if (dateString) {
      const datePipe = new DatePipe('en-US');
      return datePipe.transform(dateString, 'dd-MMM-yy');
    } else {
      return null;
    }
  }
  initProgramCategorys() {
      return this._fb.group({
        categoryName: [''],
        maxPoints: [''],
        maxScore: ['']
      });
  }
  get formData() { return <FormArray>this.myForm.get('programCategorys'); }

  addCategory() {
      const control = <FormArray>this.myForm.controls['programCategorys'];
      control.push(this.initProgramCategorys());
  }

  removeCategory(i: number) {
      const control = <FormArray>this.myForm.controls['programCategorys'];
      control.removeAt(i);
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
onSubmit() {
  if (this.myForm.valid) {
    this.submitPc(this.myForm.value, this.myForm.valid);
  } else {
    this.validateAllFormFields(this.myForm);
  }
}
  submitPc(modelPC: ProgramCreator, isValid: boolean) {
  
   
    
  }

cancelPc() {
  this.myForm.reset();
}
loadMemberInfo(event){

}
search(event){

}
myUploader(event){
  
}
}

export interface ProgramCreator {
    programName: string;
   
    startDate: string;
    endDate: string;
    programCategorys: ProgramCategorys[];
}

export interface ProgramCategorys {
    categoryName: string;
    maxPoints: number;
    maxScore: number;
}
