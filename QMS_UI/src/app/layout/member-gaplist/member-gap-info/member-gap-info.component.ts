import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { routerTransition } from '../../../router.animations';
import { Gaps, CloseGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';

@Component({
    selector: 'app-tables',
    templateUrl: './member-gap-info.component.html',
    styleUrls: ['./member-gap-info.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MemberGapInfoComponent implements OnInit {
    closeGaps: CloseGaps;
    gaps: any;
    cols: any[];
    gapId: string;
    memberId: number;
    statusTypes =  [ { label: 'Open', value: 'Open' }, { label: 'Closed', value: 'Closed' }];
    priorityTypes =  [ { label: 'High', value: 'High' }, { label: 'Low', value: 'Low' }, { label: 'Medium', value: 'Medium' }];

    constructor(private gapsService: GapsService,
        private route: ActivatedRoute) {
            this.route.params.subscribe(params => {
                this.gapId = params['gapId'];
                this.memberId = params['memberId'];
            });
        }

    ngOnInit() {
        this.gapsService.getGapsInfo(this.gapId, this.memberId).subscribe((data: CloseGaps) => {
            this.closeGaps = data;
            const gapsArray = this.closeGaps.careGaps;
            if (gapsArray.length) {
                this.gaps = gapsArray;
            }
        });
        this.cols = [
            { field: 'measureTitle', header: 'Care Gaps' },
            { field: 'intervention', header: 'Interventions' },
            { field: 'priority', header: 'Priority' },
            { field: 'payerComments', header: 'Payer Comments' },
            { field: 'providerComments', header: 'Provider Comments' },
            { field: 'status', header: 'Status' },
            { field: 'dateTime', header: 'Date & Time' },
        ];
    }
}
