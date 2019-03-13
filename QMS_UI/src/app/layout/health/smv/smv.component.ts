import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder,FormArray } from '@angular/forms';
import {GapsService }from '../../../shared/services/gaps.service';
import { DatePipe } from '@angular/common';
import { MessageService } from '../../../shared/services/message.service';
import { ActivatedRoute } from '@angular/router';
 
@Component({
  selector: 'app-smv',
  templateUrl: './smv.component.html',
  styleUrls: ['./smv.component.scss']
})
export class SmvComponent implements OnInit {
  memberId:any;
  memberData:any;
  data:any;
  options:any;
  generalData:any;
  careData:any;
  PersonaData:any;
  insuranceData:any;
  cols:any;
  constructor(private GapsService:GapsService,private _fb: FormBuilder,private msgService:MessageService,private route:ActivatedRoute) {
    this.route.params.subscribe(params => {
      this.memberId = params['memberId'];    
    });
    this.GapsService.getSpv(this.memberId).subscribe((res:any)=>{
      this.memberData = res;
    });
    this.GapsService.getSmvDataGeneral(this.memberId).subscribe((res: any)=>{
      this.generalData = res;
     // console.log(this.generalData[0])
    });
    this.GapsService.getSmvDataCare(this.memberId).subscribe((res: any)=>{
      this.PersonaData = res;
     // console.log(this.PersonaData)
    })
    this.cols =[
      {field:['goal','frequency','goalDate'],header:'Goals'},
      {field:'reward',header:'Reward'}
    ];
    console.log(this.cols)
    this.GapsService.getSmvInsuranceData(this.memberId).subscribe((res:any)=>{
      this.insuranceData = res;

    })
  }

  ngOnInit() {  
    
    this.data = {
      labels: ['IP Visits', 'OP Visits', 'ER Visits'],
      datasets: [
          {
              label: 'Visit Count',
              backgroundColor: '#20a8d8',
              borderColor: '#1E88E5',
              data: [2, 5, 1]
          },
      ]
  }
  this.options = {
      legend: {
          display: false
      },
      scales: {
          yAxes: [{
              label: 'Visists',
            ticks: {
              stepSize: 5,
              beginAtZero: true
            }
          }]
        }
  };
  }

}
