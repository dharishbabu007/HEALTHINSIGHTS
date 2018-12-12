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
  uploadedFiles: any[] = [];
  public myForm: FormGroup;
  selectedClusterId: any;
  //clusterList: any[];
  clusterList= [
    {
      label: 'Cluster 1',
      value: '1'
    },
    {
      label: 'Cluster 2',
      value: '2'
    },
    {
      label: 'Cluster 3',
      value: '3'
    },
    {
      label: 'Cluster 4',
      value: '4'
    },
    {
      label: 'Cluster 5',
      value: '5'
    },
    {
      label: 'Cluster 6',
      value: '6'
    },
    {
      label: 'Cluster 7',
      value: '7'
    }
    ];
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
        clusterData: any;
        cols: any[];
        cols1: any[];
        cols2: any[];
        cols3: any[];
        cols4: any[];
        cols5: any[];
        cols6: any[];
        cols7: any[];
  ngOnInit() {
    this.myForm = this._fb.group({
      personaName: [''],
      demographics: [''],
      motivations: [''],
      barriers: [''],
      socialMedia: [''],
      healthStatus: [''],
      goals:['']


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
onSubmit() {
  if (this.myForm.valid) {
    this.submitPc(this.myForm.value, this.myForm.valid);
  } else {
    this.validateAllFormFields(this.myForm);
  }
}
  submitPc(model,isValid: boolean) {
    if(this.selectedClusterId == null){
 this.msgService.error("please select a cluster")
    }
    else{
       this.gapsService.createPersona(model,this.selectedClusterId).subscribe((res: any )=>{
      this.msgService.success(res.message)
       });
    } 

  }

cancelPc() {
  this.myForm.reset();
}
clusterSelection(event){
  this.selectedClusterId = event.value;
  this.gapsService.getClusterData(event.value).subscribe((data: any)=>{
      this.clusterData = data;
 this.myForm.controls['personaName'].setValue(this.clusterData.clusterPersona.personaName);
 this.myForm.controls['demographics'].setValue(this.clusterData.clusterPersona.demographics);
 this.myForm.controls['motivations'].setValue(this.clusterData.clusterPersona.motivations);
 this.myForm.controls['barriers'].setValue(this.clusterData.clusterPersona.barriers);
 this.myForm.controls['socialMedia'].setValue(this.clusterData.clusterPersona.socialMedia);
 this.myForm.controls['healthStatus'].setValue(this.clusterData.clusterPersona.healthStatus);
 this.myForm.controls['goals'].setValue(this.clusterData.clusterPersona.goals);
 this.cols= data.clusterContVars.filter(item => item);
// console.log(this.cols)
/* this.cols = [
  { field: 'attribute', header: 'Attribute' },
  { field: 'min', header: 'Min' },
  { field: 'firstQuartile', header: '1st Quartile' },
  { field: 'median', header: 'Median' },
  { field: 'secondQuartile', header: '2nd Quartile' },
  { field: '', header: 'Histogram' }
];*/
 this.cols1 = data.clusterCateVars.filter(item => item.attribute =="Form of exercise")
 this.cols2 = data.clusterCateVars.filter(item => item.attribute =="Frequency of exercise")

 this.cols3 = data.clusterCateVars.filter(item => item.attribute =="Set and achieve goals")

 this.cols4 = data.clusterCateVars.filter(item => item.attribute =="Motivations")

 this.cols5 = data.clusterCateVars.filter(item => item.attribute =="Reason to not enroll")
 this.cols6 = data.clusterCateVars.filter(item => item.attribute =="Social Meida")
 this.cols7 = data.clusterCateVars.filter(item => item.attribute =="Comorbidity_Count")



});
 
  
}
onBasicUpload(event) {
  for(let file of event.files) {
      this.uploadedFiles.push(file);
      console.log(this.uploadedFiles)
  }
}
display: boolean = false;

showDialog() {
    this.display = true;
}


} 


export interface ProgramCreator {
  personaName: string,
  demographics: string,
  motivations: string,
  barriers: string,
  socialMedia: string,
  healthStatus: string,
  goals:string

}