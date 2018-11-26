import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { Router } from '@angular/router';
import { ActivatedRoute } from '@angular/router';
import { MessageService } from '../../shared/services/message.service';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { NgxPermissionsService, NgxRolesService} from 'ngx-permissions';


@Component({
  selector: 'app-viewpersonas',
  templateUrl: './view-personas.component.html',
  styleUrls: ['./view-personas.component.scss']
})
export class ViewPersonaComponent implements OnInit {

  public myForm: FormGroup;
  measureDomainList: any[];
  type: string;
  measureId: string;
  constructor(private _fb: FormBuilder,
    private router: Router,
    private route: ActivatedRoute,
  private msgService: MessageService,
private gapsService: GapsService) {
   
    this.route.params.subscribe(params => {
            this.measureId = params['measureId'];
        }); }
        membergaps: MemberCareGaps[];
        cols: any[];

  ngOnInit() {
    this.myForm = this._fb.group({
        programName: ['', [Validators.required, Validators.minLength(5)]],
        startDate: ['', [Validators.required, Validators.minLength(5)]],
        endDate: ['', [Validators.required, Validators.minLength(5)]],
      });
      this.gapsService.getlikelihoodconfusionmatric().subscribe((data: MemberCareGaps[]) => {
        this.membergaps = data;
    });

    this.cols = [
        { field: 'id', header: 'ID' },
        { field: 'zero', header: 'Zero' },
        { field: 'one', header: 'One' },
    ];
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







}


export interface ProgramCreator {
    programName: string;
   
    startDate: string;
    endDate: string;
}