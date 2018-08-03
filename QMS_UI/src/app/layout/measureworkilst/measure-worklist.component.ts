import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { MessageService } from '../../shared/services/message.service';
import { Router } from '@angular/router';
@Component({
    selector: 'app-tables',
    templateUrl: './measure_worklist.component.html',
    styleUrls: ['./measure-worklist.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MeasureworklistComponent implements OnInit {
    constructor(private gapsService: GapsService,
        private msgService: MessageService,
         public router: Router) {}
    membergaps: MemberCareGaps[];
    cols: any[];
    ngOnInit() {
        this.gapsService.getWorkList().subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        });
        this.cols = [
            { field: 'id', header: 'Measure ID', width: '10%'},
            { field: 'name', header: 'Measure Name', width: '20%' },
            { field: 'programName', header: 'Program Name', width: '20%' },
            { field: 'status', header: 'Status', width: '10%' },
            { field: 'reviewComments', header: 'Review Comments' , width: '20%'},
            { field: 'reviewedBy', header: 'Reviewed By' , width: '20%'},
        ];
    }
    copytoCreator(id, newType) {
        this.router.navigate(['/measurecreator', id, newType]);
   }
   statusClickImg(status, id) {
       this.gapsService.setMeasureStatus(id, status).subscribe( (res: any) => {
        if (res.status === 'SUCCESS') {
            this.msgService.success('Measure approved Successfully');
          } else {
            this.msgService.success(res.message);
          }
       });
   }
}
