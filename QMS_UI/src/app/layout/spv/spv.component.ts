import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';

@Component({
    selector: 'app-tables',
    templateUrl: './spv.component.html',
    styleUrls: ['./spv.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class SpvComponent implements OnInit {
    memberId: string;
    membergaps: MemberCareGaps[];
    patientProfileForm: any;
    constructor(private gapsService: GapsService, private route: ActivatedRoute) {
        this.route.params.subscribe(params => {
            if (params['memberId']) {
                this.memberId = params['memberId'];
            }
        });
    }
    ngOnInit() {
        this.gapsService.getSpv(this.memberId).subscribe((data: MemberCareGaps[]) => {
            this.patientProfileForm = data;
        });
       // console.log(pattientProfileForm.emailAddress);
        // this.patientProfileForm = [];
    }
}
