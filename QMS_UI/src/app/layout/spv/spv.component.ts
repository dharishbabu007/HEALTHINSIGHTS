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
    data: any;
    options: any;
    risk: any;
    memberId: string;
    alerts: any;
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
            
            this.risk = this.patientProfileForm.risk;
           
           //console.log( this.patientProfileForm.careGapAlerts);
          // console.log( this.patientProfileForm.careGaps);
           for(let i=0;i<this.patientProfileForm.careGaps.length;i++){
            for(let j=0;j<this.patientProfileForm.careGapAlerts.length;j++){
               // console.log(this.patientProfileForm.careGapAlerts[j])
                if(this.patientProfileForm.careGapAlerts[j].careGap== this.patientProfileForm.careGaps[i][0]){
                   // console.log(this.patientProfileForm.careGapAlerts[j].careGap == this.patientProfileForm.careGaps[i][0])
                    if(this.patientProfileForm.careGapAlerts[j].alerts){
                    this.patientProfileForm.careGaps[i].alerts = this.patientProfileForm.careGapAlerts[j].alerts;
                    }
                }
            }

           }
          // this.patientProfileForm.careGapAlerts.forEach((element) => {
          //  this.alerts = this.patientProfileForm.careGaps.filter((element1) => {
             
           //    return element1[0] != element.careGap;
           // })
          // })
       // console.log(this.patientProfileForm.careGaps)
           });
    
        this.data = {
            labels: ['IP Visits', 'OP Visits', 'ER Visits'],
            datasets: [
                {
                    label: 'Visit Count',
                    backgroundColor: '#20a8d8',
                    borderColor: '#1E88E5',
                    data: [2, 5, 1]
                },
            ]
        }
        this.options = {
            legend: {
                display: false
            },
            scales: {
                yAxes: [{
                    label: 'Visists',
                  ticks: {
                    stepSize: 5,
                    beginAtZero: true
                  }
                }]
              }
        };
       // console.log(pattientProfileForm.emailAddress);
        // this.patientProfileForm = [];
    }
}
