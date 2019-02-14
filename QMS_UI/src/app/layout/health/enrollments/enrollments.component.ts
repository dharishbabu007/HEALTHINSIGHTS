import { Component, OnInit } from '@angular/core';
import { GapsService } from "../../../shared/services/gaps.service";
import { Pipe } from '@angular/core';
import { MessageService } from '../../../shared/services/message.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-enrollments',
  templateUrl: './enrollments.component.html',
  styleUrls: ['./enrollments.component.scss']
})
export class EnrollmentsComponent implements OnInit {
  confusionmatrix: any;
  selected: any[];
  cols: any;
  cols1: any;
  cols2: any;
  selectedrow: any;
  listLength: any;
  crmLength: any;
  verifiedLength: any;
  sourceRewards: any;
  crmData: any;
  verifiedData: any;
  targetRewards: any;some1: any;
  memberId: any;
  crmButtonClick:any;
  verifyButtonClick:any;
  constructor(private gaps:GapsService,private msgService:MessageService,  private router: Router,) { 
    this.gaps.getEnrollmentData().subscribe((data: any[]) => {
      this.confusionmatrix = data;
      this.listLength = data.length;
  
  });
  this.cols = [
    { field: ['memberName','memberId','age','gender'], header: 'Member Details' },
    { field:'channel' , header: 'Preferred Channel'},
    { field:['reward1','reward2','reward3'] , header: 'Recommended Rewards'},  
    { field: 'likelihoodEnrollment', header: 'Likelihood' },
    { field: 'remarks', header: 'Remarks' },
];
this.gaps.getCRMData().subscribe((data: any[]) => {
  this.crmData = data;
  // console.log(data)
  this.crmLength = data.length;

});
this.cols1 = [
  { field: ['memberName','memberId','age','gender'], header: 'Member Details' },
  { field:'channel' , header: 'Preferred Channel'},
  { field:['reward1','reward2','reward3'] , header: 'Recommended Rewards'},  
  { field: 'likelihoodEnrollment', header: 'Likelihood' },
  { field: 'remarks', header: 'Remarks' },
];
this.gaps.getVerifiedData().subscribe((data: any[]) => {
  this.verifiedData = data;
  // console.log(data)
  this.verifiedLength = data.length;

});
this.cols2 = [
  { field: ['memberName','memberId','age','gender'], header: 'Member Details' },
  { field:'channel' , header: 'Preferred Channel'},
  { field:['reward1','reward2','reward3'] , header: 'Recommended Rewards'},  
  { field: 'likelihoodEnrollment', header: 'Likelihood' },
  { field: 'remarks', header: 'Remarks' },
];
this.gaps.getRewardsList().subscribe((res:any)=>{
  this.sourceRewards =[];
  res.forEach(element => {
   this.sourceRewards.push({label:element,value:element})
  });
  //console.log(res)
  //console.log(this.sourceRewards)
 
});

//console.log(this.cols)
  }
  ngOnInit() {
  }
  showDialog:boolean = false;
  picklist(event){
   // console.log(event)
   this.memberId = event.memberId;
    this.showDialog = true;
    this.targetRewards =[];
   
    this.some1 =[];
    this.some1.push({label: event.reward1,value:event.reward1});
    this.some1.push({label: event.reward2,value:event.reward2});
    this.some1.push({label: event.reward3,value:event.reward3});
    this.targetRewards = this.some1;
   // console.log(this.targetRewards)
 

  }
  saveChanges(data){
    console.log(data)
    let model;
    model =[];
    data.forEach(element=>{
      model.push(element.value)
    })
    console.log(model)
    this.gaps.updateRewardList(this.memberId,model).subscribe((res :any)=>{
      if(res.status=='SUCCESS'){
        this.msgService.success('updated successfully')
      }
      else{
          this.msgService.error('cannot update')
      }
    })
    
  }
  OnSubmit(data){
 // console.log(data)
  this.crmButtonClick ="crm";
  this.gaps.updateEnrollmentrow(this.crmButtonClick,data).subscribe((res:any)=>{
    if(res.status=='SUCCESS'){
      this.msgService.success('updated successfully')
      this.gaps.getEnrollmentData().subscribe((data: any[]) => {
        this.confusionmatrix = data;
        this.listLength = data.length;
    
    });
    this.cols = [
      { field: ['memberName','memberId','age','gender'], header: 'Member Details' },
      { field:'channel' , header: 'Preferred Channel'},
      { field:['reward1','reward2','reward3'] , header: 'Recommended Rewards'},  
      { field: 'likelihoodEnrollment', header: 'Likelihood' },
      { field: 'remarks', header: 'Remarks' },
  ];
  this.gaps.getCRMData().subscribe((data: any[]) => {
    this.crmData = data;
    // console.log(data)
    this.crmLength = data.length;
  
  });
  this.cols1 = [
    { field: ['memberName','memberId','age','gender'], header: 'Member Details' },
    { field:'channel' , header: 'Preferred Channel'},
    { field:['reward1','reward2','reward3'] , header: 'Recommended Rewards'},  
    { field: 'likelihoodEnrollment', header: 'Likelihood' },
    { field: 'remarks', header: 'Remarks' },
  ];
  this.gaps.getVerifiedData().subscribe((data: any[]) => {
    this.verifiedData = data;
    // console.log(data)
    this.verifiedLength = data.length;
  
  });
  this.cols2 = [
    { field: ['memberName','memberId','age','gender'], header: 'Member Details' },
    { field:'channel' , header: 'Preferred Channel'},
    { field:['reward1','reward2','reward3'] , header: 'Recommended Rewards'},  
    { field: 'likelihoodEnrollment', header: 'Likelihood' },
    { field: 'remarks', header: 'Remarks' },
  ];
    }
    else{
        this.msgService.error('cannot update')
    }

  })
  }
  verifySubmit(data){
 // console.log(data)
  this.verifyButtonClick ="verify";
  this.gaps.updateEnrollmentrow(this.verifyButtonClick,data).subscribe((res:any)=>{
    if(res.status=='SUCCESS'){
      this.msgService.success('updated successfully');
      this.gaps.getEnrollmentData().subscribe((data: any[]) => {
        this.confusionmatrix = data;
        this.listLength = data.length;
    
    });
    this.cols = [
      { field: ['memberName','memberId','age','gender'], header: 'Member Details' },
      { field:'channel' , header: 'Preferred Channel'},
      { field:['reward1','reward2','reward3'] , header: 'Recommended Rewards'},  
      { field: 'likelihoodEnrollment', header: 'Likelihood' },
      { field: 'remarks', header: 'Remarks' },
  ];
  this.gaps.getCRMData().subscribe((data: any[]) => {
    this.crmData = data;
    // console.log(data)
    this.crmLength = data.length;
  
  });
  this.cols1 = [
    { field: ['memberName','memberId','age','gender'], header: 'Member Details' },
    { field:'channel' , header: 'Preferred Channel'},
    { field:['reward1','reward2','reward3'] , header: 'Recommended Rewards'},  
    { field: 'likelihoodEnrollment', header: 'Likelihood' },
    { field: 'remarks', header: 'Remarks' },
  ];
  this.gaps.getVerifiedData().subscribe((data: any[]) => {
    this.verifiedData = data;
    // console.log(data)
    this.verifiedLength = data.length;
  
  });
  this.cols2 = [
    { field: ['memberName','memberId','age','gender'], header: 'Member Details' },
    { field:'channel' , header: 'Preferred Channel'},
    { field:['reward1','reward2','reward3'] , header: 'Recommended Rewards'},  
    { field: 'likelihoodEnrollment', header: 'Likelihood' },
    { field: 'remarks', header: 'Remarks' },
  ];
    }
    else{
        this.msgService.error('cannot update')
    }

    
  })
  }

}