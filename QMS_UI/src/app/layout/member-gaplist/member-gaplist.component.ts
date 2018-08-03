import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { Gaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';

@Component({
    selector: 'app-tables',
    templateUrl: './member-gaplist.component.html',
    styleUrls: ['./member-gaplist.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MemberGapListComponent implements OnInit {
    memberID: string;
    constructor(private gapsService: GapsService, private route: ActivatedRoute) {
        this.route.params.subscribe(params => {
            if (params['memberId']) {
                this.memberID = params['memberId'];
            }
        });
    }
    gaps: any = [];
    cols: any[];
    memberList: any;
    statusTypes =  [
        { label: 'Select', value: '' },
        { label: 'Open', value: 'Open' },
        { label: 'Closed', value: 'Closed' }
    ];
    priorityTypes =  [
        { label: 'Select', value: '' },
        { label: 'High', value: 'High' },
        { label: 'Low', value: 'Low' },
        { label: 'Medium', value: 'Medium' }
    ];

    ngOnInit() {
        if (this.memberID) {
            this.gapsService.getGaps(this.memberID).subscribe((data: any) => {
                data.qmsGicLifecycle.forEach(element => {
                    element.measureTitle = element.factHedisGapsInCare[0].measureTitle;
                });
                this.gaps = data;
                // console.log('test', data.qmsGicLifecycle);
            });
        }
        this.cols = [
            { field: 'measureTitle', header: 'Care Gaps' },
            { field: 'interventions', header: 'Interventions' },
            { field: 'priority', header: 'Priority' },
            // { field: 'payerComments', header: 'Payer Comments' },
            // { field: 'providerComments', header: 'Provider Comments' },
            { field: 'status', header: 'Status' },
            { field: 'start_date', header: 'Start Date' },
            { field: 'end_date', header: 'Last Action Date' },
            { field: 'duration', header: 'Aging' },
        ];
    }
    search(event) {
        this.gapsService.getMemberList(event.query).subscribe((data: any) => {
            this.memberList = [];
            data.forEach(element => {
                this.memberList.push(`${element.firstName} ${element.lastName} [${element.memberId}]`);
            });
        });
    }
    loadMemberInfo(memberString) {
        const pattern = /\[(.*?)\]/g;
        const match = pattern.exec(memberString);
        console.log(match, '-', memberString);
        if (match.length) {
            this.memberID = match[1];
            this.gapsService.getGaps(this.memberID).subscribe((data: any) => {
                data.qmsGicLifecycle.forEach(element => {
                    element.measureTitle = element.factHedisGapsInCare[0].measureTitle;
                });
                this.gaps = data;
                // data.forEach(element => {
                //     // const memberInfo: any = {};
                //     // memberInfo.name =  element.name;
                //     // memberInfo.memberId =  element.memberId;
                //     // memberInfo.gender =  element.gender;
                //     // memberInfo.dateOfBirth =  element.dateOfBirth;
                //     this.gaps.push(element);
                // });
            });
        }
    }
}
