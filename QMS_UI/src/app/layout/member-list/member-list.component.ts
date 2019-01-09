import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { ActivatedRoute } from '@angular/router';

@Component({
    selector: 'app-tables',
    templateUrl: './member-list.component.html',
    styleUrls: ['./member-list.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MemberListComponent implements OnInit {
    type: any;
    constructor(private gapsService: GapsService,private route: ActivatedRoute) {
        this.route.params.subscribe(param=>
        this.type = param['type']
    )
    }
    membergaps: MemberCareGaps[];
    membergaps1: MemberCareGaps[];
    cols: any[];
    cols1:any[];
    clusterList: any[];
    nameList: any[];
    loading = true;
     genderTypes =  [
        { label: 'Select', value: '' },
        { label: 'Male', value: 'Male' },
        { label: 'Female', value: 'Female' }
    ];
    ngOnInit() {
        this.gapsService.getLikelihoodMeasureDetails().subscribe((data: MemberCareGaps[]) => {
            data.forEach(element => {
                element.age = parseInt(element.age, 10);
            });
            this.membergaps = data;
            this.loading = false;
        });
        this.cols = [
            { field: 'memberId1', header: 'Member Id' },
            { field: 'memberName1', header: 'Member Name' },
            { field: 'enrollGaps1', header: 'What form of excercise do you prefer?' },
            { field: 'outOfPocketExpenses1', header: 'How frequently do you excercise?' },
            { field: 'utilizerCategory1', header: 'What motivates you for your health goals?' },
            { field: 'age1', header: 'Why would you abstain from enrolling?' },
            { field: 'amountSpend1', header: 'Do you set health goals regularly and to try to achieve them?' },
            { field: 'er1', header: 'How many people are there in your family?' },      
        ];
       
    }
    clusterSelection(event){
        console.log(event)
    }
    nameSelection(event){

    }
}
