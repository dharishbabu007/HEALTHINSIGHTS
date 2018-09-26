import { Component, OnInit } from '@angular/core';
import { ProgrameditorService } from './programeditor.service';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
import { Router } from '@angular/router';
import { DatePipe } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { MessageService } from '../../shared/services/message.service';


@Component({
  selector: 'app-programeditor',
  templateUrl: './programeditor.component.html',
  styleUrls: ['./programeditor.component.scss']
})
export class ProgrameditorComponent implements OnInit {

  public myForm: FormGroup;

  type: string;
  title: string;
  measureId: string;
  constructor(private _fb: FormBuilder,
    private ProgrameditorService: ProgrameditorService,
    private router: Router,
    private route: ActivatedRoute,
  private msgService: MessageService) {
   
    this.route.params.subscribe(params => {
            this.measureId = params['measureId'];
            this.type = params['type'];
            this.title = (this.type === '1' ) ? 'Progaram Creator' : 'Program Editor';
        }); }

  ngOnInit() {
    this.myForm = this._fb.group({
        programName: ['', [Validators.required, Validators.minLength(5)]],
        startDate: ['', [Validators.required, Validators.minLength(5)]],
        endDate: ['', [Validators.required, Validators.minLength(5)]],
        programCategorys: this._fb.array([
              this.initProgramCategorys(),
          ])
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

 
       modelPC.startDate = this.formatDate(modelPC.startDate);
      modelPC.endDate = this.formatDate(modelPC.endDate);
  
      console.log( 'Model' + JSON.stringify(modelPC));
    this.ProgrameditorService.programCreatorSubmit(modelPC).subscribe((res: any) => {

      console.log(res)
      if (res.status == 'SUCCESS') {
        this.msgService.success('Program created Successfully');
        this.myForm.reset();
      }
       else {
        console.log(res.status)
        this.msgService.error("Program already exist for ProgramName, Start Date and End Date");
      }
    } );
  
   
    
  }

cancelPc() {
  this.myForm.reset();
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
