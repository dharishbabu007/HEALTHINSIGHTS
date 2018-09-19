import { Component, OnInit, ViewChild } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Table } from 'primeng/table';
import { DatePipe } from '@angular/common';

@Component({
    selector: 'app-tables',
    templateUrl: './member-care-gaplist.component.html',
    styleUrls: ['./member-care-gaplist.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MemberCareGapListComponent implements OnInit {
    constructor(private gapsService: GapsService) {}
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
                memgpParentItem.age = item.age;
                memgpParentItem.gender = item.gender;
                memgpParentItem.countOfCareGaps = item.countOfCareGaps;
                memgpParentItem.riskGrade = item.riskGrade;
                if (item.members.length > 0) {
                    let memgpItem: any = {};
                    item.members.forEach(element => {
                        memgpItem = element;
                        memgpItem.name = item.name;
                        memgpItem.member_id = item.member_id;
                        memgpItem.age = item.age;
                        memgpItem.gender = item.gender;
                        memgpItem.countOfCareGaps = item.countOfCareGaps;
                        memgpItem.riskGrade = item.riskGrade;
                        // console.log(memgpItem);
                        this.membergaps.push(memgpItem);
                    });
                }
            });
        });


        this.cols = [
            { field: 'member_id', header: 'Member Id' },
            { field: 'name', header: 'Name' },
            { field: 'age', header: 'Age' },
            { field: 'gender', header: 'Gender' },
            { field: 'pcp', header: 'PCP' },
            { field: 'plan', header: 'Plan' },
            { field: 'countOfCareGaps', header: 'Count of Care Gaps' },
            { field: 'careGaps', header: 'Care Gaps' },
            { field: 'status', header: 'Status' },
            { field: 'riskGrade', header: 'Risk Grade' },
            { field: 'timePeriod', header: 'Last Action Date' },

        ];
 


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
}
