import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../../router.animations';
import { MemberCareGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';
import { ActivatedRoute } from '@angular/router';
import { DatePipe } from '@angular/common';

@Component({
    selector: 'app-churn',
    templateUrl: './churn-member-list.component.html',
    styleUrls: ['./churn-member-list.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})

export class ChurnMemberListComponent implements OnInit {
    churnData:any;
    cols:any;
    constructor(private gapsService: GapsService,private route: ActivatedRoute) {
    }
    
    ngOnInit() {
        this.gapsService.getLikelihoodToClurnList().subscribe((res:any)=>{
            this.churnData = [];
            this.churnData = res;
            
        })
        this.cols = [
            { field:['name','memberId','age','gender'], header: 'Member Details' },
            { field:'motivations', header: 'Motivations' },
            { field:  ['physicalActivityGoal','physicalActivityFrequency','physicalActivityDuration','calorieIintakeGoal','calorieIntakeFrequency','calorieIntakeDuration','careGap','careGapDuration'], header: 'Goals' },
            { field:'reward', header: 'Reward' },
            { field:'likelihoodTochurn', header: 'Likelihood To Churn' },
            { field:'churn', header: 'Churn' },
           
        ];
       
    }
}
 