import { Component, OnInit } from '@angular/core';
import { GapsService } from '../../shared/services/gaps.service';
@Component({
    selector: 'app-quality-central',
    templateUrl: './quality-central.component.html',
    styleUrls: ['./quality-central.component.scss'],
    providers: [GapsService]
})
export class QualityCentralComponent implements OnInit {
    programs: any;
    clinicalConditions: any;
    measureDomains: any;
    memberList: any;
    constructor(private gapsService: GapsService) {
    }

    ngOnInit() {
        this.gapsService.getPrograms().subscribe((data: any) => {
            this.programs = data;
        });
        this.gapsService.getClinicalConditions().subscribe((data: any) => {
            this.clinicalConditions = data;
        });
        this.gapsService.getMeasureDomain().subscribe((data: any) => {
            this.measureDomains = data;
        });
    }
    
}
