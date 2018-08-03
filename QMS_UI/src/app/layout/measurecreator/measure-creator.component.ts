import { Component, OnInit, LOCALE_ID  } from '@angular/core';
import { DatePipe } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { GapsService } from '../../shared/services/gaps.service';
import { MessageService } from '../../shared/services/message.service';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { Router } from '@angular/router';

@Component({
  selector: 'app-measurecreator',
  templateUrl: './measure-creator.component.html',
  styleUrls: ['./measure-creator.component.scss'],
  providers: [GapsService]
})
export class MeasurecreatorComponent implements OnInit {

  public myForm: FormGroup;

  public submitted: boolean;
  measureId: string;
  title: string;
  type: string;
  programList: any;
  measureDomainList: any;
  measureCategories: any;
  measureTypes: any;
  constructor(private _fb: FormBuilder,
    private gapsService: GapsService,
    private msgService: MessageService,
    private router: Router,
    private route: ActivatedRoute) {
        this.route.params.subscribe(params => {
            this.measureId = params['measureId'];
            this.type = params['type'];
            this.title = (this.type === '1' ) ? 'Measure Editor' : 'Measure Creator';
        });
    }

 ngOnInit() {
  this.gapsService.getPrograms().subscribe((data: any) => {
    this.programList = [];
    data.forEach(element => {
      this.programList.push({label: element, value: element});
    });
  });
  this.gapsService.getMeasureDomain().subscribe((data: any) => {
    this.measureDomainList = [];
    data.forEach(element => {
      this.measureDomainList.push({label: element.name, value: element.name});
    });
  });
  this.gapsService.getMeasureCategories().subscribe((data: any) => {
    this.measureCategories = [];
    data.forEach(element => {
      this.measureCategories.push({label: element.name, value: element.name});
    });
  });
  this.gapsService.getMeasureTypes().subscribe((data: any) => {
    this.measureTypes = [];
    data.forEach(element => {
      this.measureTypes.push({label: element.name, value: element.value});
    });
  });
   if (this.measureId) {
    this.gapsService.getMeasureInfo(this.measureId).subscribe((data: any) => {
      this.setMeasureInfo(data);
    });
   }
      this.myForm = this._fb.group({
        programName: ['', [Validators.required]],
        denominator: [],
        name: ['', [Validators.required]],
        numerator: [],
        description: [],
        targetAge: [],
        numeratorExclusions: [],
        denomExclusions: [],
        measureDomain: [],
        target: [],
        measureCategory: [],
        type: [],
        clinocalCondition: [],
        startDate: [],
        endDate: [],
        id: [],
        Decommisioned: []
      });
  }

 setMeasureInfo(measureInfo) {
   this.myForm.controls['programName'].setValue(measureInfo.programName);
   this.myForm.controls['name'].setValue(measureInfo.name);
   this.myForm.controls['description'].setValue(measureInfo.description);
   this.myForm.controls['targetAge'].setValue(measureInfo.targetAge);
   this.myForm.controls['measureDomain'].setValue(measureInfo.measureDomain);
   this.myForm.controls['measureCategory'].setValue(measureInfo.measureCategory);
   this.myForm.controls['type'].setValue(measureInfo.type);
   this.myForm.controls['clinocalCondition'].setValue(measureInfo.clinocalCondition);
   this.myForm.controls['denominator'].setValue(measureInfo.denominator);
   this.myForm.controls['denomExclusions'].setValue(measureInfo.denomExclusions);
   this.myForm.controls['numerator'].setValue(measureInfo.numerator);
   this.myForm.controls['numeratorExclusions'].setValue(measureInfo.numeratorExclusions);
   this.myForm.controls['target'].setValue(measureInfo.target);
   if (measureInfo.startDate) {
    this.myForm.controls['startDate'].setValue(new Date(measureInfo.startDate));
   }
   if (measureInfo.endDate) {
    this.myForm.controls['endDate'].setValue(new Date(measureInfo.endDate));
   }
   if (this.type === '1') {
    this.myForm.controls['id'].setValue(measureInfo.id);
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

  submitPc(model: Measurecreator, isValid: boolean) {

       this.submitted = true;
      // call API to save
      // ...
      model.status = 'New';
      model.startDate = this.formatDate(model.startDate);
      model.endDate = this.formatDate(model.endDate);
    this.gapsService.createMeasure(model).subscribe( (res: any) => {
      if (res.status === 'SUCCESS') {
        this.msgService.success('Measure created Successfully');
      } else {
        this.msgService.success(res.message);
      }
    } );
    this.router.navigateByUrl('/measureworklist');
  }

  savePc(model: Measurecreator, isValid: boolean) {

    this.submitted = true;
   // call API to save
   // ...
   model.status = 'Open';
   model.startDate = this.formatDate(model.startDate);
   model.endDate = this.formatDate(model.endDate);
   // console.log(model);
  this.gapsService.createMeasure(model).subscribe( (res: any) => {
      if (res.status === 'SUCCESS') {
        this.msgService.success('Measure saved Successfully');
      } else {
        this.msgService.success(res.message);
      }
    } );
  }
  onSubmit() {
    if (this.myForm.valid) {
      this.submitPc(this.myForm.value, this.myForm.valid);
    } else {
      this.validateAllFormFields(this.myForm);
    }
  }
  inActiveMeasure(model) {
    this.myForm.controls['endDate'].setValidators([Validators.required]);
    this.myForm.controls['endDate'].updateValueAndValidity();
    this.myForm.controls['endDate'].markAsTouched();
    model.isActive = 'N';
    model.status = 'In-active';
    model.startDate = this.formatDate(model.startDate);
    model.endDate = this.formatDate(model.endDate);
    if (this.myForm.valid) {
      this.gapsService.createMeasure(model).subscribe( (res: any) => {
        if (res.status === 'SUCCESS') {
          this.msgService.success('Measure saved Successfully');
          this.router.navigateByUrl('/measurelibrary');
        } else {
          this.msgService.success(res.message);
        }
      } );
    }
  }
  formatDate(dateString) {
    if (dateString) {
      const datePipe = new DatePipe('en-US');
      return datePipe.transform(dateString, 'dd-MMM-yy');
    } else {
      return null;
    }
  }
}


export interface Measurecreator {
    programName: string;
    denominator: string;
    name: string;
    numerator: string;
    description: string;
    targetAge: number;
    clinocalCondition: string;
    startDate: string;
    endDate: string;
    status: string;
    id: string;
   }
