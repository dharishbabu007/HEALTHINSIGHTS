import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { AnimationEvent } from '@angular/animations';
import {
  trigger,
  state,
  style,
  animate,
  transition
} from '@angular/animations';

@Component({
    selector: 'app-tables',
    templateUrl: './spv-new.component.html',
    styleUrls: ['./spv-new.component.scss'],
    animations: [routerTransition()], 
    providers: [GapsService]
})
export class SpvNewComponent implements OnInit {
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
        this.gapsService.getSpv1(this.memberId).then((data: MemberCareGaps[]) => {
            this.patientProfileForm = data;
            console.log( this.patientProfileForm.comorbiditiesCount);
        });


      
       // console.log(pattientProfileForm.emailAddress);
        // this.patientProfileForm = [];
    }
}
