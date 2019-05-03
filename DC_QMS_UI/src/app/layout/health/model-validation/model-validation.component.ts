import { Component, OnInit } from '@angular/core';
import {GapsService }from '../../../shared/services/gaps.service';
@Component({
    selector: 'app-model-validation',
    templateUrl: './model-validation.component.html',
    styleUrls: ['./model-validation.component.scss']
  })
  export class ModelValidationComponent implements OnInit {
    modelData: any;
    constructor(private GapsService:GapsService) { }

    ngOnInit() {
      this.GapsService.getModelValidationData().subscribe((res: any)=>{
       // console.log(res)
        this.modelData = res;
      })
    }
  }