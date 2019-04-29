import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../../router.animations';
import { GapsService } from '../../../shared/services/gaps.service';
import { ActivatedRoute } from '@angular/router';

@Component({
    selector: 'app-tables',
    templateUrl: './rewards-member-list.component.html',
    styleUrls: ['./rewards-member-list.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class RewardsMemberListComponent implements OnInit {
    goalsData:any;
    constructor(private gapsService: GapsService,private route: ActivatedRoute) {
    }
    cols: any[];
    rewardTypes:any;
    ngOnInit() {
        this.gapsService.getRewardssMemberList().subscribe((res:any)=>{
            this.goalsData =[]
            this.goalsData = res;
         //   console.log(this.goalsData)
        });
        this.cols = [
            { field:['name','memberId','age','gender','persona','motivations','category','goal','frequency'], header: 'Member Details' },
            { field: 'rewardRecommendation', header: 'Reccomendations' },
            { field: 'rewardSet', header: 'Rewards Set' },
            { field:'',header:'Goal Category Heading'},
            { field:['goal','frequency'],header:'Goals Set'},
            
        ];
        this.gapsService.getRewardsList().subscribe((res:any)=>{
           this.rewardTypes =[]; 
           res.forEach(element => {
            this.rewardTypes.push({label:element,value:element})
           });
        });

    }
}
