import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';

@Component({
    selector: 'app-tables',
    templateUrl: './csv1.component.html', 
     styleUrls: ['./csv1.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class Csv1Component implements OnInit {

    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router) {
        this.route.params.subscribe(params => {
            
        });
    }
    DashboardClick(){
 
    }
    membergaps: MemberCareGaps[];
    cols: any[];
    ngOnInit() {
        this.gapsService.getCsv().subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        });
        this.cols = [
            { field: 'patientId', header: 'Patient Id' },
            { field: 'patientName', header: 'Patient Name' },          
            { field: 'appointmentID', header: 'Appointment ID' },
            { field: 'appointmentDay', header: 'Appointment Day' },
            { field: 'neighbourhood', header: 'Neighbourhood' },
            { field: 'likelihood', header: 'Likelihood' },
            { field: 'noShow', header: 'No-Show' }
        ];
    }
}
