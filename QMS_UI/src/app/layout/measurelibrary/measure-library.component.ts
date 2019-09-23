import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';
import { NgxPermissionsService} from 'ngx-permissions';
import { ViewChild } from '@angular/core'
@Component({
    selector: 'app-tables',
    templateUrl: './measure-library.component.html',
    styleUrls: ['./measure-library.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MeasurelibraryComponent implements OnInit {
    @ViewChild('dt') dt: any;
    programType = 'test';
    programValue = 'test';
    perms: any;
    flag: any;
    statusTypes = [{label: 'Active', value: 'Y'}, {label: 'Decommission', value: 'N'}];
    selectedOption=['Y'];
    certifiedData:any;
    HEDIS:boolean = false;
    dropdownOptions =[{label: 'NCQA Certified',value: 'certified'},{label:'All',value:'all'}]
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
    membergaps: any[];
    cols: any[];
    cmsReportFlag: boolean;

    ngOnInit() {
        this.dt.filters = {
            isActive: { value:'Y'}
          };
          this.dt.filter(null, null, null);
        // let arrayOfValues=['Y'];
        // this.selectedOption = this.statusTypes.filter(a => arrayOfValues.includes(a.value)).map(a => a.value);
       // console.log(this.selectedOption)
       if(this.programValue == "The Healthcare Effectiveness Data and Information Set (HEDIS)"){
           this.HEDIS = true;
       }
       else if(this.programValue =="test"){
           this.HEDIS = true;
       }
       else{
           this.HEDIS = false;
       }
       this.certifiedData =[];
        this.gapsService.getLibrary(this.programType, this.programValue).subscribe((data: any[]) => {
            this.membergaps = data;
            if(this.membergaps[0].programName == 'CMS Reporting') {
                this.cmsReportFlag = true;
                this.cols = [
                    { field: 'id', header: 'Report ID' },
                    { field: 'name', header: 'Report Name' },
                    { field: 'description', header: 'Category Description' },
                    { field: 'measureCategory', header: 'Category Type' },
                    { field: 'isActive', header: 'Status'},
                ];
            }
            else {
                this.cmsReportFlag = false;
                this.cols = [
                    { field: 'id', header: 'Measure ID' },
                    { field: 'name', header: 'Measure Name' },
                    { field: 'programName', header: 'Program Name' },
                    { field: 'type', header: 'Measure Type' },
                    { field: 'steward', header: 'Measure Steward' },
                    { field: 'clinocalCondition', header: 'Clinical Condition' },
                    { field: 'isActive', header: 'Status'},
                ];
            }
        });
        
        this.permissionsService.permissions$.subscribe((permissions) => {
          this.perms = permissions;
        });
     //   console.log(this. selectedOption);
    }
    copytoCreator(id, newType) {
      
            if(this.perms['6W']){  
                if(this.perms['5R']){
                this.router.navigate(['/measurecreator', id, newType]);  
                }
            }     

    }
    filterCategory(event){
       // console.log(event.value)
        if(event.value =="certified"){
            this.gapsService.getLibrary(this.programType, this.programValue).subscribe((data: any[]) => {
                this.membergaps = [];
                data.forEach(element =>{
                    if(element.certified == 'Y'){
                        this.membergaps.push({id:element.id,name:element.name,programName:element.programName,type:element.type,steward:element.steward,clinocalCondition:element.clinocalCondition,isActive:element.isActive,certified:element.certified})
                    }
                });
              //  console.log(this.membergaps)
            });
        }
        else{
            this.gapsService.getLibrary(this.programType, this.programValue).subscribe((data: any[]) => {
                this.membergaps = data;
            });
        }
    }

   
}
