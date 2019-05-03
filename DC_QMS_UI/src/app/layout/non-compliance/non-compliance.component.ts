import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';

@Component({
    selector: 'app-tables',
    templateUrl: './non-compliance.component.html', 
     styleUrls: ['./non-compliance.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class NonComplianceComponent implements OnInit {
redirect: any = 0 ;
    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router) {
        this.route.params.subscribe(params => {
            if (params['redirect']) {
                this.redirect = params['redirect'];
            }
        });
    }
    DashboardClick(){
 
    }
    nonCompliance: any[];
    loading = true;
    cols: any[];
    ngOnInit() {
        this.gapsService.getNonComplianceOutputList().subscribe((data: MemberCareGaps[]) => {
            this.loading = false;
            this.nonCompliance = data;
        });
        this.cols = [
            { field: 'patientId', header: 'Patient Id' },
            { field: 'patientName', header: 'Patient Name' },          
            { field: 'age', header: 'Age' },
            { field: 'gender', header: 'Gender' },
            // { field: 'race', header: 'Race' },
            // { field: 'ethnicity', header: 'Ethinicity' },
            { field: 'distanceNearestHC', header: 'Distance from nearest health center' },
            { field:'ncHistroy',header:'History of non-compliance'},
            { field: 'logOdds', header: 'Compliance potential' },
            { field: 'predictedNC', header: 'Predicted non-Compliance' }
        ];
    }
}
