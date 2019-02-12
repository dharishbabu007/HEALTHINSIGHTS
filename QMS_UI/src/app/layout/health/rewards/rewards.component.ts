import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder,FormArray } from '@angular/forms';
import {GapsService }from '../../../shared/services/gaps.service';
import { DatePipe } from '@angular/common';
import { MessageService } from '../../../shared/services/message.service';

@Component({
  selector: 'app-rewards',
  templateUrl: './rewards.component.html',
  styleUrls: ['./rewards.component.scss']
})
export class RewardsComponent implements OnInit {
  public myForm: FormGroup;
  memberList:any;
  memberID: any;
  gaps: any;
  personaData: any;
  reward1: any;
  reward2: any;
  reward3: any;
  othersList: any;
  rewardsData: any;
  cols: any;
  constructor(private GapsService:GapsService,private _fb: FormBuilder,private msgService:MessageService) { }

  ngOnInit() {  
  }
  formatDate(dateString) {
    if (dateString) {
      const datePipe = new DatePipe('en-US');
      return datePipe.transform(dateString, 'dd-MMM-yy');
    } else {
      return null;
    }
  }
  onSubmit(){
    
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
        this.GapsService.getGoalsPersonaData(this.memberID).subscribe((data : any)=>{
          this.personaData = data;
        })

}

}
