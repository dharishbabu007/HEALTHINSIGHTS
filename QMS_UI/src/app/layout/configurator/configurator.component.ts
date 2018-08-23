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
    ConditionList=  [
        
        { label: 'AND', value: 'AND' },
        { label: 'OR', value: 'OR' },
        {label: 'UNION', value: 'UNION'}
    ];
    TableNameList: any; 
    ColumnNameList: any;
    tableRepository: any;
    public configForm: FormGroup;
    constructor(private gapsService: GapsService, private _fb: FormBuilder) {
        this.configForm = this._fb.group({
            expression: [''],
            remark: ['', [Validators.required]],
            conditionList: this._fb.array([
                this.conditionParamForm(),
            ])
          });
        this.gapsService.getTableName().subscribe((data: any) => {
            this.TableNameList = [];
            this.tableRepository = data;
            data.forEach(element => {
                this.TableNameList.push({label: element.name, value: element.name});
            });
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

  
  

    }
    filterColumn(event){
        this.ColumnNameList = [];
        const columnArray = this.tableRepository.filter(item => item.name === event.value);
        columnArray[0].columnList.forEach(element => {
          this.ColumnNameList.push({label: element.name, value: element.name});
        });
    }
}
