import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../../router.animations';
import { MemberCareGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';
import { ActivatedRoute } from '@angular/router';
import { DatePipe } from '@angular/common';

@Component({
    selector: 'app-tables',
    templateUrl: './goals-member-list.component.html',
    styleUrls: ['./goals-member-list.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})

export class GoalsMemberListComponent implements OnInit {
    goalsData:any;
    constructor(private gapsService: GapsService,private route: ActivatedRoute) {
    }
    cols: any[];
    
    ngOnInit() {
        this.gapsService.getGoalsMemberList().subscribe((res:any)=>{
            this.goalsData =[]
            res.forEach( item => { 
                const temp: any ={};
                temp.name = item.name;
                temp.memberId = item.memberId;
                temp.age = item.age;
                temp.gender = item.gender;
                temp.persona = item.persona;
                if(item.goalSet.physicalActivityGoal){
                temp.physicalActivityGoal = item.goalSet.physicalActivityGoal;
                }
                if(item.goalSet.physicalActivityFrequency){
                temp.physicalActivityFrequency = item.goalSet.physicalActivityFrequency;
                }
                if(item.goalSet.physicalActivityDate){
                temp.physicalActivityDate = item.goalSet.physicalActivityDate;
                }
                if(item.goalSet.calorieIntakeGoal){
                temp.calorieIntakeGoal = item.goalSet.calorieIntakeGoal;
                }
                if(item.goalSet.calorieIntakeFrequency){
                temp.calorieIntakeFrequency = item.goalSet.calorieIntakeFrequency;
                }
                if(item.goalSet.calorieIntakeDate){
                temp.calorieIntakeDate = item.goalSet.calorieIntakeDate;
                }
                if(item.goalSet.careGap){
                temp.careGap = item.goalSet.careGap;
                }
                if(item.goalSet.careGapDate){
                temp.careGapDate = item.goalSet.careGapDate;
                }
                if(item.goalRecommendation.physicalActivityGoal){
                temp.physicalActivityGoal1 = item.goalRecommendation.physicalActivityGoal;
                }
                if(item.goalRecommendation.physicalActivityFrequency){
                temp.physicalActivityFrequency1 = item.goalRecommendation.physicalActivityFrequency;
                }
                if(item.goalRecommendation.physicalActivityDate){
                temp.physicalActivityDate1 = item.goalRecommendation.physicalActivityDate;
                }
                if(item.goalRecommendation.calorieIntakeGoal){
                temp.calorieIntakeGoal1 = item.goalRecommendation.calorieIntakeGoal;
                }
                if(item.goalRecommendation.calorieIntakeFrequency){
                temp.calorieIntakeFrequency1 = item.goalRecommendation.calorieIntakeFrequency;
                }
                if(item.goalRecommendation.calorieIntakeDate){
                temp.calorieIntakeDate1 = item.goalRecommendation.calorieIntakeDate;
                }
                if(item.goalRecommendation.careGap){
                temp.careGap1 = item.goalRecommendation.careGap;
                }
                if(item.goalRecommendation.careGapDate){
                temp.careGapDate1 = item.goalRecommendation.careGapDate;
                }
                this.goalsData.push(temp)
            });
            //console.log(this.goalsData)
        });
        this.cols = [
            { field:['name','memberId','age','gender','persona'], header: 'Member Details' },
            { field:'', header: 'Action' },
            { field:  ['physicalActivityGoal1','physicalActivityFrequency1','physicalActivityDate1','calorieIntakeGoal1','calorieIntakeFrequency1','calorieIntakeDate1','careGap1','careGapDate1'], header: 'Reccomendations' },
            { field: ['physicalActivityGoal','physicalActivityFrequency','physicalActivityDate','calorieIntakeGoal','calorieIntakeFrequency','calorieIntakeDate','careGap','careGapDate'], header: 'Goals Set' }
        ];
      //  console.log()

    }
}
 