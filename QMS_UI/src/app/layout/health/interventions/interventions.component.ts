import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder,FormArray } from '@angular/forms';
import {GapsService }from '../../../shared/services/gaps.service';
import { DatePipe } from '@angular/common';
import { MessageService } from '../../../shared/services/message.service';
import { ActivatedRoute } from '@angular/router';
import { Intervention } from './interventions.component';
import { Router } from '@angular/router';

@Component({
  selector: 'app-interventions',
  templateUrl: './interventions.component.html',
  styleUrls: ['./interventions.component.scss']
})
export class InterventionComponent implements OnInit {
  midList: any;
  goalList: any;
  public myForm: FormGroup;
  memberList:any;
  memberID: any;
  gaps: any;
  paramId:any;
  personaData: any;
  goalsData:any;
  qualitymeasureId:any;
  constructor(private GapsService:GapsService,private _fb: FormBuilder,private msgService:MessageService,private route: ActivatedRoute,    private router: Router,) {
    this.route.params.subscribe(params => {
      this.paramId = params['memberId'];    
  });
   }

  ngOnInit() {  
    this.myForm = this._fb.group({
      startDate:[''],
      physicalActivityGoal:[''],
      physicalActivityFrequency:[''],
      physicalActivityDate:[''],
      calorieIntakeGoal:[''],
      calorieIntakeFrequency:[''],
      calorieIntakeDate:[''],
      careGap:[''],
      careGapDate:[''],
      physicalIntervention:[''],
      calorieIntervention:[''],
      careIntervention:['']

    });
    if(this.paramId){
      this.GapsService.getGoalsRecommendationData(this.paramId).subscribe((res: any)=>{
        this.goalsData =[];
        this.goalsData = res;
        console.log(this.goalsData);
        this.loadMemberInfo(this.paramId);
      })
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
  onSubmit(model){
    console.log(model);
    model.memberId = this.memberID;
  this.GapsService.createIntervention(model).subscribe((res:any)=>{
    if(res.status == 'SUCCESS'){
      this.msgService.success(res.message);
      this.myForm.reset();
    }
    else {
      this.msgService.error(res.message);
    }
  })
  }
  search(event) {
    this.GapsService.getMemberList(event.query).subscribe((data: any) => {
        this.memberList = [];
        data.forEach(element => {
            this.memberList.push(`${element.memberId}`);
        });
    });
}
loadMemberInfo(memberString) {
    //const pattern = /\[(.*?)\]/g;
   // const match = pattern.exec(memberString);
   // console.log(memberString);
        this.memberID = memberString;
        this.GapsService.getGoalsMemberDetails(this.memberID).subscribe((data: any) => {
            this.gaps = data;
            this.qualitymeasureId=data.qualityMeasureId;
          
        });
        this.GapsService.getGoalsPersonaData(this.memberID).subscribe((data : any)=>{
          this.personaData = data;
       //   console.log(this.personaData)
        })
        this.GapsService.getGoalsRecommendationData(this.memberID).subscribe((res: any)=>{
          this.goalsData =[];
          this.goalsData = res;
        //  console.log(this.goalsData);
          this.myForm.controls['physicalActivityGoal'].setValue( this.goalsData.physicalActivityGoal);
          this.myForm.controls['physicalActivityFrequency'].setValue( this.goalsData.physicalActivityFrequency)
          if(this.goalsData.physicalActivityDate){
          this.myForm.controls['physicalActivityDate'].setValue(new Date( this.goalsData.physicalActivityDate))
          }
          this.myForm.controls['calorieIntakeGoal'].setValue( this.goalsData.calorieIntakeGoal)
          this.myForm.controls['calorieIntakeFrequency'].setValue( this.goalsData.calorieIntakeFrequency)
          if(this.goalsData.calorieIntakeDate){
          this.myForm.controls['calorieIntakeDate'].setValue(new Date(  this.goalsData.calorieIntakeDate))
        }
          this.myForm.controls['careGap'].setValue( this.goalsData.careGap)
          if(this.goalsData.careGapDate){
          this.myForm.controls['careGapDate'].setValue(new Date(  this.goalsData.careGapDate))
        }
        })

  }
  closeGap(){
    if(this.memberID){
      this.router.navigate(['/member-gap/',this.qualitymeasureId,this.memberID]);
    }

  }

}
export interface Intervention {

}

