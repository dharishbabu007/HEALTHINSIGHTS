import { Component, OnInit, ViewChild } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Table } from 'primeng/table';
import { ActivatedRoute } from '@angular/router';


@Component({
    selector: 'app-tables',
    templateUrl: './noshowG.component.html',
    styleUrls: ['./noshowG.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class NoShowGapListComponent implements OnInit {
    programType = 'test';
    programValue = 'test';
    constructor(private gapsService: GapsService,private route: ActivatedRoute,) {

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

    genderTypes =  [
        { label: 'Select', value: '' },
        { label: 'Male', value: 'M' },
        { label: 'Female', value: 'F' }
    ];

 @ViewChild('dt') private _table: Table;
  dateFilters: any;
    ngOnInit() {


        this.gapsService. getNoshowGaps().subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        });

        this.cols = [
            { field: 'patientId', header: 'Patient ID' },
            { field: 'name', header: 'Name' },
            { field: 'appointmentId', header: 'appointment ID' },
            { field: 'gender', header: 'Gender' },
            { field: 'dayClass', header: 'DayClass' },
            { field: 'appointmentDay', header: 'AppointmentDay' },
            { field: 'age', header: 'Age' },
            { field: 'logOdds', header: 'LogOdds' },
            { field: 'countCareGaps', header: 'Count of Care Gaps' },
            { field: 'riskGrade', header: 'Risk Grade' },
            

        ];
 


    }
  
}