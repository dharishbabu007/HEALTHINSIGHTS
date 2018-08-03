import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';
@Component({
    selector: 'app-tables',
    templateUrl: './measure-library.component.html',
    styleUrls: ['./measure-library.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MeasurelibraryComponent implements OnInit {
    programType = 'test';
    programValue = 'test';
    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router) {
        this.route.params.subscribe(params => {
            if (params['type']) {
                this.programType = params['type'];
            }
            if (params['value']) {
                this.programValue = params['value'];
            }
        });
    }
    membergaps: MemberCareGaps[];
    cols: any[];
    ngOnInit() {
        this.gapsService.getLibrary(this.programType, this.programValue).subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        });
        this.cols = [
            { field: 'id', header: 'Measure ID' },
            { field: 'name', header: 'Measure Name' },
            { field: 'programName', header: 'Program Name' },
            { field: 'type', header: 'Measure Type' },
            { field: 'steward', header: 'Measure Steward' },
            { field: 'clinocalCondition', header: 'Clinical Condition' },
            { field: 'isActive', header: 'Status'}
        ];
    }
    copytoCreator(id, newType) {
         this.router.navigate(['/measurecreator', id, newType]);
    }
}
