import { Component, OnInit } from '@angular/core';
import { ProgramcreatorService } from './programcreator.service';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
import { Router } from '@angular/router';
import { DatePipe } from '@angular/common';
import { ActivatedRoute } from '@angular/router';


@Component({
  selector: 'app-programcreator',
  templateUrl: './programcreator.component.html',
  styleUrls: ['./programcreator.component.scss']
})
export class ProgramcreatorComponent implements OnInit {

  public myForm: FormGroup;

  public submitted: boolean;
  type: string;
  title: string;
  measureId: string;
  constructor(private _fb: FormBuilder,
    private programCreatorService: ProgramcreatorService,
    private router: Router,
    private route: ActivatedRoute) {

    this.route.params.subscribe(params => {
            this.measureId = params['measureId'];
            this.type = params['type'];
            this.title = (this.type === '1' ) ? 'Progaram Editor' : 'Program Creator';
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

  submitPc(modelPC: ProgramCreator, isValid: boolean) {

       this.submitted = true;
       modelPC.startDate = this.formatDate(modelPC.startDate);
      modelPC.endDate = this.formatDate(modelPC.endDate);
      // call API to save
      // ...
      console.log( 'Model' + JSON.stringify(modelPC));
    this.programCreatorService.programCreatorSubmit(modelPC).subscribe( model => console.log('Succeessfully Created Program Creator'));
    this.router.navigateByUrl('/dashboard');
  }

  savePc(modelPC: ProgramCreator, isValid: boolean) {

    this.submitted = true;
     modelPC.startDate = this.formatDate(modelPC.startDate);
   modelPC.endDate = this.formatDate(modelPC.endDate);
   // call API to save
   // ...
   console.log('Model SavePC ' + JSON.stringify(modelPC));
 this.programCreatorService.programCreatorSubmit(modelPC).subscribe( model => console.log('Succeessfully Created Program Creator'));
}


cancelPc() {
    this.router.navigateByUrl('/dashboard');
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
