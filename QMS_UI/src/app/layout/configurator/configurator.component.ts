import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
import { GapsService } from '../../shared/services/gaps.service';
import { Condition, Configurator } from '../../shared/services/gaps.data';
@Component({
    selector: 'app-configurator',
    templateUrl: './configurator.component.html',
    styleUrls: ['./configurator.component.scss'],
    providers: [GapsService]
})
export class ConfiguratorComponent implements OnInit {
    programs: any;
    clinicalConditions: any;
    measureDomains: any;
    memberList: any;
    public configForm: FormGroup;
    constructor(private gapsService: GapsService, private _fb: FormBuilder) {
        this.configForm = this._fb.group({
            expression: [''],
            remark: ['', [Validators.required]],
            conditionList: this._fb.array([
                this.conditionParamForm(),
            ])
          });
    }
    conditionParamForm() {
        return this._fb.group({
            andOrCondition: [''],
            tableName: [''],
            columnName: [''],
            expression: ['']
          });
    }
    addCondition() {
        const control = <FormArray>this.configForm.controls['conditionList'];
        control.push(this.conditionParamForm());
    }

    removeCondition(i: number) {
        const control = <FormArray>this.configForm.controls['conditionList'];
        control.removeAt(i);
    }
    get formConditionList() { return <FormArray>this.configForm.get('conditionList'); }
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
