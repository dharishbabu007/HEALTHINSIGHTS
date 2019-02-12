import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder,FormArray } from '@angular/forms';
import {GapsService }from '../../../shared/services/gaps.service';
import { DatePipe } from '@angular/common';
import { MessageService } from '../../../shared/services/message.service';

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
  physicalActivityGoalList: any;
  physicalActivityFrequencyList: any;
  calorieIntakeGoalList: any;
  calorieIntakeFrequencyList: any;
  careGapList: any;
  personaData: any;
  constructor(private GapsService:GapsService,private _fb: FormBuilder,private msgService:MessageService) { }

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
    this.GapsService.getPhysicalActivityFrequency().subscribe((res:any)=>{
      this. physicalActivityFrequencyList=[];
      res.forEach(element =>{
        this.physicalActivityFrequencyList.push({label:element,value:element})
      })
    });
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
   // console.log(model);
    this.GapsService.createGoals(model).subscribe((res:any)=>{
      if(res.status == 'SUCCESS'){
        this.msgService.success(res.message)
      }
      else {
        this.msgService.error(res.error.message);
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
          //  console.log(this.gaps)
        });
        this.GapsService.getGoalsCareGap(this.memberID).subscribe((data:any)=>{
          this.careGapList =[];
         // console.log(data)
          data.forEach(element=>{
            this.careGapList.push({label:element,value:element})
          })
        });
        this.GapsService.getGoalsPersonaData(this.memberID).subscribe((data : any)=>{
          this.personaData = data;
        })

}

}
