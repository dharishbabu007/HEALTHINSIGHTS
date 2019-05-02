import { Component, OnInit, LOCALE_ID  } from '@angular/core';
import { DatePipe } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { GapsService } from '../../shared/services/gaps.service';
import { MessageService } from '../../shared/services/message.service';
import { FormGroup, FormControl, Validators, FormBuilder,FormArray} from '@angular/forms';
import { Router } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { DomSanitizer } from '@angular/platform-browser';
import * as FileSaver from 'file-saver';
@Component({
  selector: 'app-measurecreator',
  templateUrl: './measure-creator.component.html',
  styleUrls: ['./measure-creator.component.scss'],
  providers: [GapsService]
})
export class MeasurecreatorComponent implements OnInit {
  downloadJsonHref:any;
  value: Date;
  public myForm: FormGroup;
  public myForm1: FormGroup;
  public configForm: FormGroup;
  public denominatorExclusion: FormGroup;
  public numerator: FormGroup;
  public numeratorExclusion: FormGroup;
  disableForm = false;
  disableOnCopy = false;
  public submitted: boolean;
  measureId: string;
  title: string;
  type: string;
  Repositry: any;
  programId: any;
  programList: any;
  measureDomainList: any;
  measureCategoriesList: any;
  measureCategories: any;
  measureTypes: any;
  samplingButton: boolean = false;
  mrssList: any;
  mrssSampleList: any;
  LobNames:any;
  ProductNames:any;

  dataSoucreTypes = [{label:"Admin",value:1},{label:"ECDS",value:2},{label:"Hybrid",value:3},{label:"Survey",value: 4}];


  ConditionList=  [
        
    { label: 'AND', value: 'AND' },
    { label: 'OR', value: 'OR' },
    {label: 'UNION', value: 'UNION'}
];
TableNameList: any; 
ColumnNameList: any;
tableRepository: any;
measureData: any;

fileUrl:any;

  constructor(private _fb: FormBuilder,
    private gapsService: GapsService,
    private msgService: MessageService,
    private router: Router,
    private route: ActivatedRoute,
  private DomSanitizer:DomSanitizer) {
        this.route.params.subscribe(params => {
            this.measureId = params['measureId'];
            this.type = params['type'];
            this.title = (this.type === '1' ) ? 'Measure Editor' : 'Measure Creator';
        });
        this.myForm = this._fb.group({
          programName: ['', [Validators.required]],
          denominator: [],
          name: ['', [Validators.required]],
          numerator: [],
          description: [],
    
          targetAge: [],

          numeratorExclusions: [],
          denomExclusions: [],
          measureDomain: [],
          target: [''],
          measureCategory: [],
          type: [],
          clinocalCondition: [],
          startDate: [],
          endDate: [],
          id: [],
          Decommisioned: [],
          p50:[],
          p90:[],
          measureSourceId:[],
          collectionSource:[],
          product:[],
          payer:[]
        });
        this.myForm1 = this._fb.group({
          sampleSize:[''],
          overSampleRate:['']
        });










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

















    membergaps: MemberCareGaps[];
    cols: any[];
    cols2: any[];
 ngOnInit() {
  this.ProductNames=[];
  this.gapsService.getDropDownPrograms().subscribe((data: any) => {
    this.programList = [];
    this.Repositry =data;
    data.forEach(element => {
      this.programList.push({label: element.name, value: element.name});
    });
   

  });
  this.gapsService.getMeasureDomain().subscribe((data: any) => {
    this.measureDomainList = [];
    data.forEach(element => {
      this.measureDomainList.push({label: element.name, value: element.name});
    });
  });
  this.gapsService.getMeasureCategories().subscribe((data: any) => {
    this.measureCategoriesList = data;
  });
  this.gapsService.getMeasureTypes().subscribe((data: any) => {
    this.measureTypes = [];
    data.forEach(element => {
      this.measureTypes.push({label: element.name, value: element.name});
    });
  });
  this.gapsService.getloblist().subscribe((data: any)=>{
    this.LobNames =[];
    data.forEach(element =>{
      this.LobNames.push({label:element.name,value:element.value})
    })
  });
  this.gapsService.getproductlist().subscribe((data:any)=>{
    this.ProductNames =[];
    data.forEach(element =>{
      this.ProductNames.push({label:element,value:element})
    })
  });
   if (this.measureId) {
    this.gapsService.getMeasureInfo(this.measureId).subscribe((data: any) => {
      this.measureData =[];
      this.measureData = data;
      this.setMeasureInfo(data);
    // this.downloadFile(data)
    });
   }
   this.gapsService.getMrssSampleList().subscribe((res: any)=>{
    this.mrssSampleList =[];
   this.mrssSampleList = res;
    });
    this.cols2=[
      {field:'rate',header:'Rate'},
      {field:'sampleSize',header:'Sample Size'},

    ]
   this.gapsService.getMrssList().subscribe((data: MemberCareGaps[])=>{
     this.mrssList = data;
    });
   this.cols=[
     {field:'measureName',header:'Measure Name'},
     {field:'measureCategary',header:'Measure category'},
     {field:'medicaid',header:'Medicaid'},
     {field:'commercial',header:'Commercial'},
     {field:'medicare',header:'Medicare'},
     {field:'sample_size',header:'Sample Size'},
     {field:'rand',header:'Rand'}
   ]
   
  }
 
 setMeasureInfo(measureInfo) {
//  console.log(measureInfo)
   if (measureInfo.isActive === 'N' && this.type== '1') {
    this.myForm.disable();
    this.disableForm = true;
    this.disableOnCopy = true;
   }
   if (this.type== '2'){
   this.disableOnCopy = true;
   }
   this.myForm.controls['programName'].setValue(measureInfo.programName);
   this.myForm.controls['name'].setValue(measureInfo.name);
   this.myForm.controls['description'].setValue(measureInfo.description);
   this.myForm.controls['targetAge'].setValue(measureInfo.targetAge);
   this.myForm.controls['measureDomain'].setValue(measureInfo.measureDomain);
   this.myForm.controls['type'].setValue(measureInfo.type);
   this.myForm.controls['clinocalCondition'].setValue(measureInfo.clinocalCondition);
   this.myForm.controls['denominator'].setValue(measureInfo.denominator);
   this.myForm.controls['denomExclusions'].setValue(measureInfo.denomExclusions);
   this.myForm.controls['numerator'].setValue(measureInfo.numerator);
   this.myForm.controls['numeratorExclusions'].setValue(measureInfo.optionalExclusion);
   this.myForm.controls['target'].setValue(measureInfo.target);
   this.myForm.controls['p50'].setValue(measureInfo.p50);
   this.myForm.controls['p90'].setValue(measureInfo.p90);
   this.myForm.controls['collectionSource'].setValue(measureInfo.collectionSource);
   this.myForm1.controls['sampleSize'].setValue(measureInfo.mrss);
   this.myForm1.controls['overSampleRate'].setValue(measureInfo.overFlowRate);
    this.myForm.controls['payer'].setValue(measureInfo.payer);
    this.myForm.controls['product'].setValue(measureInfo.product)
   const temp =this.Repositry.filter(element =>element.name == measureInfo.programName);
   this.gapsService.getMeasureCategory(temp[0].value).subscribe((res:any)=> {
    const data = res;
   // console.log(data)
    this.measureCategories =[];
    data.forEach(element => {
      this.measureCategories.push({label:element,value: element });
      this.myForm.controls['measureCategory'].setValue(measureInfo.measureCategory);
    });
    
  });

   if (measureInfo.startDate) {
    this.myForm.controls['startDate'].setValue(new Date(measureInfo.startDate));
   }
   if (measureInfo.endDate) {
    this.myForm.controls['endDate'].setValue(new Date(measureInfo.endDate));
   }
   if (this.type === '1') {
    this.myForm.controls['id'].setValue(measureInfo.id);
   }
 }
//  onLobSelection(event){
//    if(event.value.length <1){
//     this.gapsService.getproductlist(event.value).subscribe((data:any)=>{
//       data.forEach(element =>{
//         this.ProductNames.push({label:element,value:element})
//       })
//     })
//    }
//    else{
//      let temp = event.value.length;
//      let lobid = event.value[temp-1]
//     this.gapsService.getproductlist(lobid).subscribe((data:any)=>{
//       data.forEach(element =>{
//         this.ProductNames.push({label:element,value:element})
//       })
//     })
//    }
//  }
downloadForm(event){
  var mediaType = 'text/plain';
  event = JSON.stringify(event);
  event = event.split(/,/g).join('\n');
  event = event.split("{").join("");
  event = event.split("}").join("");
  event = event.split('"').join("");
  event = event.split('"').join("");
  var blob = new Blob([event], {type: "text/plain;charset=utf-8"});
  var filename = 'test.doc';
  FileSaver.saveAs(blob, filename);
}

 validateAllFormFields(formGroup: FormGroup) {
    Object.keys(formGroup.controls).forEach(field => {
    const control = formGroup.get(field);
    if (control instanceof FormControl) {
      control.markAsTouched({ onlySelf: true });
    } else if (control instanceof FormGroup) {
      this.validateAllFormFields(control);
    }
  });

}
temppayer:any;
  submitPc(model: Measurecreator, isValid: boolean) {
this.temppayer =[];
    if (this.myForm.valid) {
  
       this.submitted = true;
       
      // call API to save
      // ...
      model.status = 'New';
     // model.target = parseInt(model.target, 10);
      model.startDate = this.formatDate(model.startDate);
      model.endDate = this.formatDate(model.endDate);
      model.mrss =  this.myForm1.controls['sampleSize'].value;
      model.overFlowRate =  this.myForm1.controls['overSampleRate'].value;
      // console.log(model)
    this.gapsService.createMeasure(model).subscribe( (res: any) => {
      if (res.status === 'SUCCESS') {
        this.msgService.success('Measure created Successfully');
        this.router.navigateByUrl('/measureworklist?fetch');
      } else {
        this.msgService.error(res.message);
      }
    } );
  }
  else{
    this.validateAllFormFields(this.myForm);
  }
 
  }
  savePc(model: Measurecreator, isValid: boolean) {

    this.submitted = true;
   // call API to save
   // ...
   if (this.myForm.valid) {
   model.status = 'Open';
  // model.target = parseInt(model.target, 10);
   model.startDate = this.formatDate(model.startDate);
   model.endDate = this.formatDate(model.endDate);
  this.gapsService.createMeasure(model).subscribe( (res: any) => {
      if (res.status === 'SUCCESS') {
        this.msgService.success('Measure saved Successfully');
        this.router.navigateByUrl('/measureworklist?fetch');
      } else {
        this.msgService.error(res.message);
      }
    } );
  }
  else{
    this.validateAllFormFields(this.myForm);
  }


}
  onSubmit() {
    if (this.myForm.valid) {
      this.submitPc(this.myForm.value, this.myForm.valid);
    } else {
      this.validateAllFormFields(this.myForm);
      this.submitPc(this.myForm.value, this.myForm.valid);
    }
  }
  OnSubmitConfigurator(){
    
  }
  inActiveMeasure(model) {
    this.myForm.controls['endDate'].setValidators([Validators.required]);
    this.myForm.controls['endDate'].updateValueAndValidity();
    this.myForm.controls['endDate'].markAsTouched();
    model.isActive = 'N';
    model.status = 'In-active';
    //model.target = parseInt(model.target, 10);
    model.startDate = this.formatDate(model.startDate);
    model.endDate = this.formatDate(model.endDate);
    if (this.myForm.valid) {
      this.gapsService.createMeasure(model).subscribe( (res: any) => {
        if (res.status === 'SUCCESS') {
          this.msgService.success('Measure saved Successfully');
          this.router.navigateByUrl('/measurelibrary?fetch');
        } else {
          this.msgService.error(res.message);
        }
      } );
    }
  }
  formatDate(dateString) {
    if (dateString) {
      const datePipe = new DatePipe('en-US');
      return datePipe.transform(dateString, 'dd-MMM-yy');
    } else {
      return null;
    }
  } 
  filterCategory(event) {
    this.programId = this.Repositry.filter(item => item.name === event.value);

    this.gapsService.getDate(this.programId[0].value).subscribe((res:any) => {
  
    this.myForm.controls['startDate'].setValue(new Date(res.startDate));
    this.myForm.controls['endDate'].setValue(new Date(res.endDate));
    
  
    }); 
    this.gapsService.getMeasureCategory(this.programId[0].value).subscribe((res:any)=> {
      const data = res;
      // console.log(data)
      this.measureCategories =[];
      data.forEach(element => {
        this.measureCategories.push({label:element,value: element })
      })
      
    }); 
  }
  dataSourceSelected(event)
  {
    if(event.value=="3"){
      this.samplingButton = true;
    }
    else{
      this.samplingButton = false;
    }
  }
  dialogBox:boolean = false;
  showDialogBox()
  {
    this.dialogBox = true;
  }
  closedialog(){
    this.dialogBox = false;
  }
}


export interface Measurecreator {
    programName: string;
    denominator: string;
    name: string;
    numerator: string;
    description: string;
    targetAge: number;
    clinocalCondition: string;
    startDate: string;
    endDate: string;
    status: string;
    id: string;
    target: number;
    p50:number;
    p90:number;
    datacollection:number;
    mrss:string;
    overFlowRate: string;
    payer:any;
    product:any;
   }
