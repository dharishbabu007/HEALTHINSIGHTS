import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder, FormArray } from '@angular/forms';
import { GapsService } from '../../shared/services/gaps.service';
import { Condition, Configurator } from '../../shared/services/gaps.data';
import { QueryBuilderConfig } from 'angular2-query-builder';
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
    options: any;
    temp: any;
    tablevalue: any = null;
    public configForm: FormGroup;
    public denominatorExclusion: FormGroup;
    public numerator: FormGroup;
    public numeratorExclusion: FormGroup;
    query = {
        condition: 'and',
        rules: [
          {field: 'age', operator: '<=', value: 'Bob'},
          {field: 'gender', operator: '>=', value: 'm'}
        ]
      };
      config: QueryBuilderConfig = {
        fields: {
          "age": {name: "Age", type: 'number'},
          "gender": {
            name: "Gender",
            type: 'category',
            options: [
              {name: "Male", value: "m"},
              {name: "Female", value: "f"}
            ]
          },
          "name": {name: "Name", type: 'string'},
          "educated": {name: "College Degree?", type: 'boolean'},
          "birthday": {name: "Birthday", type: 'date'},
          "school": {name: "School", type: 'string', nullable: true},
          "occupation": {
            name: "Occupation",
            type: 'string',
            options: [
              {name: "Student", value: "student"},
              {name: "Teacher", value: "teacher"},
              {name: "Unemployed", value: "unemployed"},
              {name: "Scientist", value: "scientist"}
            ]
          }
        }
      };

    constructor(private gapsService: GapsService, private _fb: FormBuilder) {
        this.configForm = this._fb.group({
            expression: [''],
            remark: ['', [Validators.required]],
            conditionList: this._fb.array([
                this.conditionParamForm(),
            ])
          });
          this.denominatorExclusion = this._fb.group({
            expressionDE: [''],
            remarkDE: ['', [Validators.required]],
            DEList: this._fb.array([
                this.DEForm(),
            ])
          });
          this.numerator = this._fb.group({
            expressionN: [''],
            remarkN: ['', [Validators.required]],
            NList: this._fb.array([
                this.NForm(),
            ])
          });
          this.numeratorExclusion = this._fb.group({
            expressionNE: [''],
            remarkNE: ['', [Validators.required]],
            NEList: this._fb.array([
                this.NEForm(),
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
            expression: [''],
            dropdownvalue1:[''],
            dropdownvalue2:['']
          });
    }
    DEForm(){
        return this._fb.group({
            andOrConditionDE: [''],
            tableNameDE: [''],
            columnNameDE: [''],
            expressionDE: ['']
          });
    }
    NForm(){
        return this._fb.group({
            andOrConditionN: [''],
            tableNameN: [''],
            columnNameN: [''],
            expressionN: ['']
          });
    }
    NEForm(){
        return this._fb.group({
            andOrConditionNE: [''],
            tableNameNE: [''],
            columnNameNE: [''],
            expressionNE: ['']
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
    addConditionDE() {
        const control = <FormArray>this.denominatorExclusion.controls['DEList'];
        control.push(this.DEForm());
    }

    removeConditionDE(i: number) {
        const control = <FormArray>this.denominatorExclusion.controls['DEList'];
        control.removeAt(i);
    }
    addConditionN() {
        const control = <FormArray>this.numerator.controls['NList'];
        control.push(this.NForm());
    }

    removeConditionN(i: number) {
        const control = <FormArray>this.numerator.controls['NList'];
        control.removeAt(i);
    }
    addConditionNE() {
        const control = <FormArray>this.numeratorExclusion.controls['NEList'];
        control.push(this.NEForm());
    }

    removeConditionNE(i: number) {
        const control = <FormArray>this.numeratorExclusion.controls['NEList'];
        control.removeAt(i);
    }
    get formConditionList() { return <FormArray>this.configForm.get('conditionList'); }

    get formConditionListDE() { return <FormArray>this.denominatorExclusion.get('DEList'); }
    get formConditionListN() { return <FormArray>this.numerator.get('NList'); }

    get formConditionListNE() { return <FormArray>this.numeratorExclusion.get('NEList'); }

    ngOnInit() {

  
  

    }
    filterColumn(event, index){
        this.ColumnNameList = [];
        this.options = [];
        this.tablevalue =[];
        const columnArray = this.tableRepository.filter(item => item.name === event.value);
        this.options[index] = columnArray[0].columnList;
       // this.options[index] = columnArray[0].columnList;
        columnArray[0].columnList.forEach(element => {
        this.ColumnNameList.push({label: element.name, value: element.name});
        });
        this.tablevalue =(<FormArray>this.configForm.controls['conditionList']).controls[index]['controls']['tableName'].value;
        console.log(this.options)
    }
    changedValue(event){
        console.log(event)
        this.temp = event.value;
    }
    OnSubmit(){
        console.log(this.configForm.value);
        console.log(this.denominatorExclusion.value);
        console.log(this.numerator.value);
        console.log(this.numeratorExclusion.value);
        
    }
}
