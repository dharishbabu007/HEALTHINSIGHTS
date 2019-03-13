import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder,FormArray } from '@angular/forms';
import {GapsService }from '../../../shared/services/gaps.service';
import { DatePipe } from '@angular/common';
import { MessageService } from '../../../shared/services/message.service';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-goals',
  templateUrl: './goals.component.html',
  styleUrls: ['./goals.component.scss']
})
export class GoalsComponent implements OnInit {
  midList: any;
  goalList: any;
  public myForm: FormGroup;
  memberList:any;
  memberID: any;
  gaps: any;
  paramId:any;
  physicalActivityGoalList: any;
  physicalActivityFrequencyList: any;
  calorieIntakeGoalList: any;
  calorieIntakeFrequencyList: any;
  careGapList: any;
  personaData: any;
  goalsData:any;
  goal:any;
  calorieIntake:any;
  physicalActivityRefId:any;
  calorieIntakeRefId:any;
  qualitymeasureId:any;
  constructor(private GapsService:GapsService,private _fb: FormBuilder,private msgService:MessageService,private route: ActivatedRoute) {
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
      careGapDate:['']

    });
    this.GapsService.getPhysicalActivityGoal().subscribe((res:any)=>{
      this. physicalActivityGoalList=[];
      res.forEach(element =>{
        this.physicalActivityGoalList.push({label:element,value:element})
      })
    });
    // this.GapsService.getPhysicalActivityFrequency().subscribe((res:any)=>{
    //   this. physicalActivityFrequencyList=[];
    //   res.forEach(element =>{
    //     this.physicalActivityFrequencyList.push({label:element,value:element})
    //   })
    // });
    this.GapsService. getCalorieIntakeGoal().subscribe((res:any)=>{
      this. calorieIntakeGoalList=[];
      res.forEach(element =>{
        this.calorieIntakeGoalList.push({label:element,value:element})
      })
    });
    this.GapsService.getCalorieIntakeFrequency().subscribe((res:any)=>{
      this. calorieIntakeFrequencyList=[];
      res.forEach(element =>{
        this.calorieIntakeFrequencyList.push({label:element,value:element})
      })
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
    model.name = this.gaps.name;
    model.age = this.gaps.age;
    model.gender = this.gaps.gender;
    model.memberId = this.memberID;
    model.physicalActivityDate = this.formatDate(model.physicalActivityDate);
    model.calorieIntakeDate = this.formatDate(model.calorieIntakeDate);
    model.careGapDate = this.formatDate(model.careGapDate);
    model.persona = this.personaData.personaName;
    model.preferredGoal =this.personaData.goals;
    model.currentCalorieIntake = this.personaData.measureCalorieIntake;
    model.physicalActivityId = this.physicalActivityRefId;
    model.calorieIntakeId = this.calorieIntakeRefId;
    model.qualityMeasureId = this.qualitymeasureId;
   // console.log(model);
    this.GapsService.createGoals(model).subscribe((res:any)=>{
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
          
        });
        this.GapsService.getGoalsCareGap(this.memberID).subscribe((data:any)=>{
          this.careGapList =[];
         // console.log(data)
          data.forEach(element=>{ 
            this.careGapList.push({label:element.name,value:element.value})
          })
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
  physicalActivitySelection(event){
   // console.log(event.value);
   this.goal = event.value;
    this.GapsService.getPhysicalActivityFrequency(event.value).subscribe((res:any)=>{
      this. physicalActivityFrequencyList=[];
         res.forEach(element =>{
         this.physicalActivityFrequencyList.push({label:element.frequency,value:element.frequency})
     })
    })
  }
  pAFrequencySelection(event){
    this.GapsService.getPhysicalActivityFrequencyId(this.goal,event.value).subscribe((res:any)=>{
      this.physicalActivityRefId= res[0].refId
    })

  }
  cIGoalSelection(event){
    this.calorieIntake = event.value;
  }
  cIFrequencySelection(event){
    this.GapsService.getCalorieIntakeFrequencyId(this.calorieIntake,event.value).subscribe((res:any)=>{
      this.calorieIntakeRefId= res[0].refId
    })

  }
  careGapSelection(event){
    this.qualitymeasureId = event.value;
  }

}
