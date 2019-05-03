import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../../router.animations';
import { MemberCareGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';
import { ActivatedRoute } from '@angular/router';
import { DatePipe } from '@angular/common';

@Component({
    selector: 'app-recommend',
    templateUrl: './recommend-member-list.component.html',
    styleUrls: ['./recommend-member-list.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})

export class RecommendMemberListComponent implements OnInit {
    recommendData:any;
    cols:any;
    constructor(private gapsService: GapsService,private route: ActivatedRoute) {
    }
    ngOnInit() {
        this.gapsService.getLikelihoodToRecommendList().subscribe((res:any)=>{
            this.recommendData =[];
           this.recommendData = res;
        })
        this.cols = [
            { field:['name','member_id','age','gender','persona'], header: 'Member Details' },
            { field:'motivations', header: 'Motivations' },
            { field:  ['physicalActivityGoal','physicalActivityFrequency','physicalActivityDuration','calorieIintakeGoal','calorieIntakeFrequency','calorieIntakeDuration','careGap','careGapDuration'], header: 'Goals' },
            { field:['performancePhysicalActivity','performanceCalorieIntake','performanceCareGap'], header: 'Performance' },
            { field:'likelihoodToRecommend', header: 'Likelihood To Recommend' },
            { field:'recommendation', header: 'Recommend' },
           
        ];
      
    }
}
 