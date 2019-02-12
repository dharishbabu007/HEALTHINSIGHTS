import { Component, OnInit } from '@angular/core';
import { GapsService } from "../../../shared/services/gaps.service";
import { Pipe } from '@angular/core';

@Component({
  selector: 'app-enrollments',
  templateUrl: './enrollments.component.html',
  styleUrls: ['./enrollments.component.scss']
})
export class EnrollmentsComponent implements OnInit {
  confusionmatrix: any;
  selected: any[];
  cols: any;
  selectedcar: any;
  listLength: any;
  crmLength: any;
  verifiedLength: any;
  sourceRewards: any;
  targetRewards: any;some1: any;
  constructor(private gaps:GapsService) { 
    this.gaps.getEnrollmentData().subscribe((data: any[]) => {
      this.confusionmatrix = data;
      this.listLength = data.length;
  
  });
  this.cols = [
    { field: ['memberId','age','gender'], header: 'Member Details' },
    { field:'channel' , header: 'Preferred Channel'},
    { field:['reward1','reward2','reward3'] , header: 'Recommended Rewards'},  
    { field: 'likelihoodEnrollment', header: 'Likelihood' },
    { field: 'remarks', header: 'Remarks' },
];

//console.log(this.cols)
  }
  ngOnInit() {
  }
  showDialog:boolean = false;
  picklist(event){
   // console.log(event)
    this.showDialog = true;
    this.targetRewards =[];
    this.sourceRewards =[];
    this.some1 =[];
    this.some1.push({label: event.reward1,value:event.reward1});
    this.some1.push({label: event.reward2,value:event.reward2});
    this.some1.push({label: event.reward3,value:event.reward3});
    this.targetRewards = this.some1;

  }

}