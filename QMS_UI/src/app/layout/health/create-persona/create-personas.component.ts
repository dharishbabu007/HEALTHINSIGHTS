import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder,FormArray } from '@angular/forms';
import { Router } from '@angular/router';
import { ActivatedRoute } from '@angular/router';
import { MessageService } from '../../../shared/services/message.service';
import { MemberCareGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';
import { NgxPermissionsService, NgxRolesService} from 'ngx-permissions';

@Component({
  selector: 'app-createpersonas',
  templateUrl: './create-personas.component.html',
  styleUrls: ['./create-personas.component.scss']
})
export class CreatePersonaComponent implements OnInit {
  uploadedFiles: any[] = [];
  public myForm: FormGroup;
  public personaData: FormGroup;
  selectedClusterId: any;
  data: any;
  options: any;
  clusterData: any;
  cols: any[];
  cols1: any[];
  cols2: any[];
  clusterList: any;
  type: string;
  measureId: string; 
  ageGroupList: string;
  educationList: any;
  occupationList: any;
  familySizeList: any;
  SelectedFile: File = null;
  incomeList: any;
  socialMediaList: any;
  addictionList: any;
  clusterLength: any;
  personaFormData:any;
  constructor(private _fb: FormBuilder,
    private router: Router,
    private route: ActivatedRoute,
  private msgService: MessageService,
private gapsService: GapsService) {
   
    this.route.params.subscribe(params => {
            this.measureId = params['measureId'];
        }); 
     // console.log(this.labels1)
     this.myForm = this._fb.group({
      personaName: [''],
      bio:[''],
      motivations: [''],
      barriers: [''],
      socialMedia: [''],
      healthStatus: [''],
      goals:[''],
      ageGroup: [''],
      education:[''],
      occupation:[''],
      income:[''],
      addiction:[''],
      familySize: ['']


      });
      
}
       
  ngOnInit() {
      this.personaData = this._fb.group({
        conditionList: this._fb.array([
          this.conditionParamForm(),
      ])
      })
      this.gapsService.getclusterlist().subscribe((res: any) =>{
        this.clusterList =[];
       // console.log(res)
        res.forEach(item => {
            this.clusterList.push({label: "cluster "+item,value: item});
            });
          //  console.log()
            this.clusterLength =[];
            if(this.clusterLength){
              for(let i=1;i<this.clusterList.length;i++){
                this.clusterLength.push(this.clusterList[i])
              this.personaFormDataFunc(i,this.clusterList[i].value);
              }
            }
      });
     
  }
personaFormDataFunc(i,value){
  this.personaFormData =[];
  this.gapsService.getClusterFormData(value).subscribe((res: any)=>{              
    this.personaFormData.push(res.clusterPersona);
   // console.log(this.personaFormData)
  });


}
smallCardClick(event){
  //console.log(event);
  event = {label:event,value:event}
  this.clusterSelection(event)
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
handleFileInput(event) {
this.SelectedFile= <File>event.target.files[0];
}
conditionParamForm(){
  return this._fb.group({
    createdClusterName: [''],
    createdPersonaName:[''],
    createdAgeGroup:['']
  });
}

addCondition() {
  const control = <FormArray>this.personaData.controls['conditionList'];
  control.push(this.conditionParamForm());
}

removeCondition(i: number) {
  const control = <FormArray>this.personaData.controls['conditionList'];
  control.removeAt(i);
}
get formConditionList() { return <FormArray>this.personaData.get('conditionList'); }
onBasicUpload(event){
  
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
      const fd = new FormData();
      fd.append('file', this.SelectedFile);
      // if(this.SelectedFile.type=="image/png"||this.SelectedFile.type=="image/jpeg"){

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
    
// this.cols1 = [];
//  this.cols1.push(data.filter(item => item.featureName =="form_of_exercise"));  
//  this.cols1.push(data.filter(item => item.featureName =="set_and_achieve_health_goal"));
//  this.cols1.push(data.filter(item => item.featureName =="take_care_of_loved_ones"));
//  this.cols1.push(data.filter(item => item.featureName =="ideal_health_goal"));
// this.cols =[];
// this.cols.push(data.filter(item => item.featureName =="frequency_of_exercise"));
//  this.cols.push(data.filter(item => item.featureName =="measure_calorie_intake"));
//  this.cols.push(data.filter(item => item.featureName =="reason_to_not_enroll"));
//  this.cols2= data.filter(item => item);
    
this.cols1 = [];
 this.cols1.push(data.filter(item => item.featureName =="Age"));
 this.cols1.push(data.filter(item => item.featureName =="Education"));
 this.cols1.push(data.filter(item => item.featureName =="Addictions"));
 this.cols1.push(data.filter(item => item.featureName =="Family Size"));

this.cols =[];
this.cols.push(data.filter(item => item.featureName =="What are your motivations for leading a healthy life?"));
 this.cols.push(data.filter(item => item.featureName =="Why would you not be interested in enrolling into the wellness program?"));
 this.cols.push(data.filter(item => item.featureName =="Do you actively use social media?"));
 //this.cols.push(data.filter(item => item.featureName =="How many chronic diseases do you suffer from?"));
//console.log(this.cols)
 this.cols2= data.filter(item => item);

});
if(event.value == 1){
  this.myForm.controls['personaName'].setValue('Lazy Doer');
 //this.myForm.controls['demographics'].setValue(this.clusterData.clusterPersona.demographics);
 this.myForm.controls['motivations'].setValue('Need a Partner');
 this.myForm.controls['barriers'].setValue('The Frequency of excersice');
 this.myForm.controls['socialMedia'].setValue('Use Frequently');
 this.myForm.controls['healthStatus'].setValue('Email');
 this.myForm.controls['goals'].setValue('To avoid diseases');
 this.myForm.controls['bio'].setValue('Lazy doers are not proactive in nature. These are people who are not intrinsically motivated.');
 //this.myForm.controls['demographics'].setValue(this.clusterData.clusterPersona.demographics);
 this.myForm.controls['ageGroup'].setValue('30-35');
 console.log(this.myForm.controls['ageGroup'].value)
 this.myForm.controls['education'].setValue('High school, Graduation');
 this.myForm.controls['income'].setValue('$40,000 - $60,000');
 this.myForm.controls['occupation'].setValue('Desk job, Unemployed');
 this.myForm.controls['addiction'].setValue('Drinking, Smoking');
 this.myForm.controls['familySize'].setValue('3-6');

}
else{

this.gapsService.getClusterFormData(event.value).subscribe((data : any)=>{
  this.clusterData = data;
  this.myForm.controls['personaName'].setValue(this.clusterData.clusterPersona.personaName);
 //this.myForm.controls['demographics'].setValue(this.clusterData.clusterPersona.demographics);
 this.myForm.controls['motivations'].setValue(this.clusterData.clusterPersona.motivations);
 this.myForm.controls['barriers'].setValue(this.clusterData.clusterPersona.barriers);
 this.myForm.controls['socialMedia'].setValue(this.clusterData.clusterPersona.socialMedia);
 this.myForm.controls['healthStatus'].setValue(this.clusterData.clusterPersona.healthStatus);
 this.myForm.controls['goals'].setValue(this.clusterData.clusterPersona.goals);
 this.myForm.controls['bio'].setValue(this.clusterData.clusterPersona.bio);
 //this.myForm.controls['demographics'].setValue(this.clusterData.clusterPersona.demographics);
 this.myForm.controls['ageGroup'].setValue(this.clusterData.clusterPersona.demoAgeGroup);
 console.log(this.myForm.controls['ageGroup'].value)
 this.myForm.controls['education'].setValue(this.clusterData.clusterPersona.demoEducation);
 this.myForm.controls['income'].setValue(this.clusterData.clusterPersona.demoIncome);
 this.myForm.controls['occupation'].setValue(this.clusterData.clusterPersona.demoOccupation);
 this.myForm.controls['addiction'].setValue(this.clusterData.clusterPersona.demoAddictions);
 this.myForm.controls['familySize'].setValue(this.clusterData.clusterPersona.demoFamilySize);


}); 
}
}


display: boolean = false;

showDialog(rowData) {
    this.display = true;
   // console.log(rowData.clusterId,rowData.featureName);
    
  this.gapsService.getgraphdata(rowData.clusterId,rowData.featureName).subscribe((res: any)=>
   {
      //  console.log(res.x);
        //console.log(res.y);
      this.data = {
          labels: res.x,
          datasets: [
              {
                  label: 'First Dataset',
                  data: res.y,
                  fill: false,
                  borderColor: '#4bc0c0'
              },
           
          ]
      }
      this.options = {
        legend: {
            display: false
        }
       };
   }
   );
   

}
displayModal: boolean = false;
showDialogModal(rowData) {
  this.display = true;
 // console.log(rowData.clusterId,rowData.featureName);
  
this.gapsService.getgraphdata(rowData.clusterId,rowData.featureName).subscribe((res: any)=>
 {
    //  console.log(res.x);
      //console.log(res.y);
    this.data = {
        labels: res.x,
        datasets: [
            {
                label: 'First Dataset',
                data: res.y,
                fill: false,
                borderColor: '#4bc0c0'
            },
         
        ]
    }
    this.options = {
      legend: {
          display: false
      }
     };
 }
 );
 

}

displayAllData: boolean = false;
showAllDialog(){
  this.displayAllData = true;
}

} 


export interface CreatePersona {
  personaName: string,
  bio: string,
  motivations: string,
  barriers: string,
  socialMedia: string,
  healthStatus: string,
  goals:string,

  education:number,
  occupation:string,
  income:string,
  addiction:string,
  familySize: string

}