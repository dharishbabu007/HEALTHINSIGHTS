import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';

@Component({
    selector: 'app-tables',
    templateUrl: './member-care-gaplist.component.html',
    styleUrls: ['./member-care-gaplist.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MemberCareGapListComponent implements OnInit {
    constructor(private gapsService: GapsService) {}
    membergaps = [];
    cols: any[];
    loading = true;
    genderTypes =  [
        { label: 'Select', value: '' },
        { label: 'Male', value: 'M' },
        { label: 'Female', value: 'F' }
    ];
    ngOnInit() {
        this.gapsService.getMemberGaps().subscribe((data: any) => {
            this.loading = false;
            this.membergaps = [];
            data.forEach( item => {
                const memgpParentItem: any = {};
                memgpParentItem.name = item.name;
                memgpParentItem.member_id = item.member_id;
                memgpParentItem.age = item.age;
                memgpParentItem.gender = item.gender;
                memgpParentItem.countOfCareGaps = item.countOfCareGaps;
                memgpParentItem.riskGrade = item.riskGrade;
                if (item.members.length > 0) {
                    let memgpItem: any = {};
                    item.members.forEach(element => {
                        memgpItem = element;
                        memgpItem.name = item.name;
                        memgpItem.member_id = item.member_id;
                        memgpItem.age = item.age;
                        memgpItem.gender = item.gender;
                        memgpItem.countOfCareGaps = item.countOfCareGaps;
                        memgpItem.riskGrade = item.riskGrade;
                        // console.log(memgpItem);
                        this.membergaps.push(memgpItem);
                    });
                }
            });
        });
        this.cols = [
            { field: 'member_id', header: 'Member Id' },
            { field: 'name', header: 'Name' },
            { field: 'age', header: 'Age' },
            { field: 'gender', header: 'Gender' },
            { field: 'pcp', header: 'PCP' },
            { field: 'plan', header: 'Plan' },
            { field: 'countOfCareGaps', header: 'Count of Care Gaps' },
            { field: 'careGaps', header: 'Care Gaps' },
            { field: 'status', header: 'Status' },
            { field: 'riskGrade', header: 'Risk Grade' },
            { field: 'timePeriod', header: 'Last Action Date' },

        ];
    }
}
