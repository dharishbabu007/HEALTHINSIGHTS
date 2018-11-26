import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';
import { NgxPermissionsService, NgxRolesService} from 'ngx-permissions';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
@Component({
    selector: 'app-tables',
    templateUrl: './create-personas.component.html',
    styleUrls: ['./create-personas.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class CreatePersonasComponent implements OnInit {
     
    programType = 'test';
    programValue = 'test';
    statusTypes = [{label: 'Active', value: 'Approved'}, {label: 'Decommission', value: 'In-active'}];
    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router,  private permissionsService: NgxPermissionsService,) {
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
     /*   this.gapsService.getLibrary(this.programType, this.programValue).subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        }); */
        this.cols = [
            { field: 'id', header: 'Measure ID' },
            { field: 'name', header: 'Measure Name' },
            { field: 'programName', header: 'Program Name' },
            { field: 'type', header: 'Measure Type' },
            { field: 'steward', header: 'Measure Steward' },
            { field: 'clinocalCondition', header: 'Clinical Condition' },
            { field: 'status', header: 'Status'}
        ];
    }
}

 /*{ field: 'persona', header: 'Persona' },
            { field: 'description', header: 'Description' },
            { field: 'id', header: 'Cluster' }, 
            { field: 'attb1', header: 'Attb 1' },
            { field: 'attb2', header: 'Attb 2' },
            { field: 'attb3', header: 'Attb 3' },
            { field: 'attb4', header: 'Attb 4'} */