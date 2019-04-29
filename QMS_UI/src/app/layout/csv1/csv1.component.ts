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
modelName:any;
title:any;
loading = true;
    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router) {
        this.route.params.subscribe(params => {
            this.modelName = params['selectedModel'];
        });
    }
    DashboardClick(){
 
    }
    membergaps: MemberCareGaps[];
    cols: any[];
    ngOnInit() {
      //  console.log(this.modelName)
        switch(this.modelName){
            case "noshow": 
            {
                this.title = "No-Show Likelihood List";
                this.gapsService.getCsv().subscribe((data: MemberCareGaps[]) => {
                    this.loading= false;
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
                break;
            }
            case "nc":
            {
                this.title = "Non-compliance Prediction";
                this.gapsService.getNcCsv().subscribe((data: MemberCareGaps[]) => {
                    this.membergaps = data;
                });
                this.cols = [
                    { field: 'patient_id', header: 'Patient Id' },
                    { field: 'age', header: 'Age' },          
                    { field: 'gender', header: 'Gender' },
                    { field: 'distance_from_nearest_heath_center', header: 'Distance from nearest health center' },
                    { field: 'no_of_non_compliance_measures', header: 'Number of non-compliance measures' },
                    { field: 'histroy_of_non_compliance', header: 'History of non-compliance' },
                    { field: 'logodds', header: 'Logodds' },
                    { field: 'predicted_non_compliance', header: 'Predicted non-compliance' }
                ];
                break;
            }
            case "lhc":
            {
                this.title = "Likelihood to Churn";
                this.gapsService.getLhcCsv().subscribe((data: MemberCareGaps[]) => {
                    this.membergaps = data;
                });
                this.cols = [
                    { field: 'patient_id', header: 'Patient Id' },
                    { field: 'age', header: 'Age' },          
                    { field: 'gender', header: 'Gender' },
                    { field: 'distance_from_nearest_heath_center', header: 'Distance from nearest health center' },
                    { field: 'no_of_non_compliance_measures', header: 'Number of non-compliance measures' },
                    { field: 'histroy_of_non_compliance', header: 'History of non-compliance' },
                    { field: 'logodds', header: 'Logodds' },
                    { field: 'predicted_non_compliance', header: 'Predicted non-compliance' }
                ];
                break;
            }
            case "lhe":
            {
                this.title = "Likelihood to Enroll";
                this.gapsService.getLheCsv().subscribe((data: MemberCareGaps[]) => {
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
                break;
            }
            case "persona":
            {
                this.title = "Clustering";
                break;
            }
        }
        
    }
}
