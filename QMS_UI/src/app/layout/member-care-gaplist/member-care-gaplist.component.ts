import { Component, OnInit, ViewChild } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Table } from 'primeng/table';
import { DatePipe } from '@angular/common';
import { Router } from '@angular/router';
import * as XLSX from 'xlsx';

@Component({
    selector: 'app-tables',
    templateUrl: './member-care-gaplist.component.html',
    styleUrls: ['./member-care-gaplist.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MemberCareGapListComponent implements OnInit {
    @ViewChild('dt') dt: any;
    constructor(private gapsService: GapsService,    private router: Router) {}
    membergaps = [];
    cols: any[];
    loading = true;
    genderTypes =  [
        { label: 'Select', value: '' },
        { label: 'Male', value: 'M' },
        { label: 'Female', value: 'F' }
    ];

 @ViewChild('dt') private _table: Table;
  dateFilters: any;
    ngOnInit() {

 this._table.filterConstraints['my'] = (value, filter): boolean => {
            // Make sure the value and the filter are Dates

           
            return filter.getTime() == filter.getTime();
        }


        this.gapsService.getMemberGaps().subscribe((data: any) => {
            this.loading = false;
            this.membergaps = [];
            data.forEach( item => {
                const memgpParentItem: any = {};
                memgpParentItem.name = item.name;
                memgpParentItem.member_id = item.member_id;
                memgpParentItem.measureSK = item.measureSK;
                memgpParentItem.age = item.age;
                memgpParentItem.gender = item.gender;
                memgpParentItem.countOfCareGaps = item.countOfCareGaps;
                memgpParentItem.riskGrade = item.riskGrade;
                memgpParentItem.compliancePotential = item.compliancePotential;
                if (item.members.length > 0) {
                    let memgpItem: any = {};
                    item.members.forEach(element => {
                        memgpItem = element;
                        memgpItem.name = item.name;
                        memgpItem.member_id = item.member_id;
                        memgpItem.member_id = item.member_id;
                        memgpItem.measureSK = item.measureSK;
                        memgpItem.age = item.age;
                        memgpItem.gender = item.gender;
                        memgpItem.countOfCareGaps = item.countOfCareGaps;
                        memgpItem.riskGrade = item.riskGrade;
                        memgpItem.compliancePotential = item.compliancePotential;
                       // console.log(memgpItem);
                        this.membergaps.push(memgpItem);
                    });
                }
            });
            //console.log()
        });


        this.cols = [
            // { field: 'member_id', header: 'Member Id' },
            // { field: 'name', header: 'Name' },
            // { field: 'age', header: 'Age' },
            // { field: 'gender', header: 'Gender' },
            { field:['name','member_id','age','gender'],header:'Member Details'},
            { field: 'pcp', header: 'PCP' },
            { field: 'plan', header: 'Plan' },
            { field: 'countOfCareGaps', header: 'Count of Care Gaps' },
            { field: 'careGaps', header: 'Care Gaps' },
            { field: 'status', header: 'Status' },
            { field: 'compliancePotential', header:'Compliance Potential' },
            { field: 'riskGrade', header: 'Risk Grade' },
            { field: 'timePeriod', header: 'Last Action Date' },

        ];
 


    }
    exporting(){
        // this.dt.exportCSV(this.membergaps, "Contracts.csv");
        const workBook = XLSX.utils.book_new(); // create a new blank book
        const workSheet = XLSX.utils.json_to_sheet(this.membergaps);
    
        XLSX.utils.book_append_sheet(workBook, workSheet, 'data'); // add the worksheet to the book
        XLSX.writeFile(workBook, 'Member_Care_Gaps_Registry.xlsx'); // initiate a file download in browser
    }
    dateFilter(dateString){
        console.log('date', this.formatDate(dateString))
        this._table.value.filter((data)=>{
            data.timePeriod == this.formatDate(dateString)
        })
    }
    formatDate(dateString) {
        if (dateString) {
          const datePipe = new DatePipe('en-US');
          return datePipe.transform(dateString, 'yyyy-MM-dd');
        } else {
          return null;
        }
      }

      mitscreen(rowData){
        // console.log(rowData);
            this.router.navigate(['/pat-screen', rowData.member_id, rowData.measureSK,rowData.plan]);  
     //
      }
}
