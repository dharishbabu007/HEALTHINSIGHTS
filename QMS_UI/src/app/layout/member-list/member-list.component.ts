import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';

@Component({
    selector: 'app-tables',
    templateUrl: './member-list.component.html',
    styleUrls: ['./member-list.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MemberListComponent implements OnInit {
    constructor(private gapsService: GapsService) {}
    membergaps: MemberCareGaps[];
    cols: any[];
    loading = true;
     genderTypes =  [
        { label: 'Select', value: '' },
        { label: 'Male', value: 'Male' },
        { label: 'Female', value: 'Female' }
    ];
    ngOnInit() {
        this.gapsService.getMeasureDetails().subscribe((data: MemberCareGaps[]) => {
            data.forEach(element => {
                element.age = parseInt(element.age, 10);
            });
            this.membergaps = data;
            this.loading = false;
        });
        this.cols = [
            { field: 'id', header: 'Member Id' },
            { field: 'age', header: 'Age' },
            { field: 'gender', header: 'Gender' },
            { field: 'name', header: 'Name' },
            { field: 'hccScore', header: 'MRA Score' },
            { field: 'amount', header: 'Amount' },
            { field: 'reason', header: 'Reason' },
        ];
    }
}
