import { Component, OnInit } from '@angular/core';
import {GapsService }from '../../../shared/services/gaps.service';

@Component({
  selector: 'app-director_home',
  templateUrl: './director_home.component.html',
  styleUrls: ['./director_home.component.scss']
})
export class DirectorHomeComponent implements OnInit {
  coordinator:boolean = false;
  analyst: boolean = false;
  qulaity_director: boolean = false;
  roleList: any;
  rolename: any;
  Enrollments: any;
  Progress: any;
  Goals: any;
  Rewards: any;
  Satisfaction: any;
  Interventions: any;
  Persona: any;
  Effectiveness: any;
  Model_Validation: any;
  enrollmentChart: any;
  enrollmentChart1: any;
  cliamsChart: any;
  cliamsChart1: any;
  memberHealthChart: any;
  memberHealthChart1: any;
  brandChart: any;
  brandChart1: any;
  riskChart: any;
  riskChart1: any;
  options: any;
  options1: any;
  enrollmentyoy: any;
  cliamsyoy: any;
  memberHealthyoy: any;
  brandyoy: any;
  demo_user: Boolean = false;
  enrollmentPercent: any;
  claimsPercent:any;
  memberHealthPercent:any;
  brandLoyaltyPercent:any;

  constructor(private GapsService:GapsService) { 
     this.riskChart = {
      labels:  ['Y','N'],
      datasets: [
          {
              data: [50,40],
              backgroundColor: [
                  "#1D7FFD",
                  "#E4E5E6"
                 
              ],
              hoverBackgroundColor: [
                  "#1D7FFD",
                  "#E4E5E6"
              ]
          }]    
      };
      this.riskChart1 = {
        labels: ['January', 'February', 'March', 'April', 'May', 'June', 'July'],
        datasets: [
            {
              label: '2019',
                data: [5, 6, 8, 1, 3, 12, 1],
                fill: false,
                borderColor: '#4bc0c0'
            },
            {
                label: '2018',
                data: [28, 48, 40, 19, 86, 27, 90],
                fill: false,
                borderColor: '#565656'
            }
        ]
    }
    //   this.options = {
    //   //   title: {
    //   //     display: true,
    //   //     text: 'Realtime Data',
    //   //     fontSize: 16
    //   // },
    //   // legend: {
    //   //     display: false,
    //   //     position: 'right'
    //   // }
    //   elements: {
    //     center: {
    //     text: 'Desktop',
    //     color: '#36A2EB', //Default black
    //     fontStyle: 'Helvetica', //Default Arial
    //     sidePadding: 15 //Default 20 (as a percentage)
    //   }
    // }
    // };
    this.options = {
      legend: {
        labels: {
          boxWidth: 10
      },
          position: 'bottom'
      }
  };
  this.options1 = {
    legend: {labels: {
      boxWidth: 10
  },
      position: 'bottom'
    },
    scales: {
      yAxes: [{
        ticks: {
          stepSize: 5,
          beginAtZero: true,  
          display: false
        },
          gridLines: {
            drawBorder: false,
              display: false
          }
      }],
      xAxes: [{
          gridLines: {
            drawBorder: false,
            display: false
          },
          ticks: {
          beginAtZero: true, 
            display: false
          }
      }]
  },
  elements : { line : { tension : 0.001  } }
};
  }

  ngOnInit() {
    var user = JSON.parse(localStorage.getItem('currentUser'));
    this.GapsService.getRoleList().subscribe((data: any) => {
      this.roleList =[];
      let rolename1 = data.filter(item => item.value === user.roleId);
      this.rolename = rolename1[0].name;
     // console.log(this.rolename)
      if(this.rolename =="Care Coordinator"){
        this.coordinator = true;
      }
      else if(this.rolename =="Business Analyst"){
        this.analyst = true;
      }
      else if(this.rolename == "Quality_Director"){
        this.qulaity_director = true;
      }
      else if(this.rolename == "demo"){
        this.demo_user = true;
      }
      else{
        this.coordinator = true;
      }
   });  
   this.GapsService.getHealthyLandingData().subscribe((res: any)=>
  {
    this.Enrollments = [];
    this.Progress=[];
    this.Goals=[];
    this.Rewards=[];
    this.Satisfaction=[];
    this.Interventions=[];
    this.Persona=[];
    this.Effectiveness=[];
    this.Model_Validation=[];
  
   // console.log(res)
      res.forEach(element => {
      if(element.type == "Enrollments"){
        this.Enrollments.push({description:element.description,value:element.value,title:element.title})
      }
      if(element.type == "Rewards"){
        this.Rewards.push({description:element.description,value:element.value,title:element.title})
      }
      if(element.type == "Goals"){
        this.Goals.push({description:element.description,value:element.value,title:element.title})
      }
      if(element.type == "Progress"){
        this.Progress.push({description:element.description,value:element.value,title:element.title})
      }
      if(element.type == "Satisfaction"){
        this.Satisfaction.push({description:element.description,value:element.value,title:element.title})
      }
      if(element.type == "Interventions"){
        this.Interventions.push({description:element.description,value:element.value,title:element.title})
      }
      if(element.type == "Persona")
      {
        this.Persona.push({description:element.description,value:element.value,title:element.title})
      }
      if(element.type == "Effectiveness")
      {
        this.Effectiveness.push({description:element.description,value:element.value,title:element.title})
      }
      if(element.type == "Model Validation")
      {
        this.Model_Validation.push({description:element.description,value:element.value,title:element.title})
      }

    });
  //  console.log(this.Persona)

  });
  this.GapsService.getObjectiveEnrollment().subscribe((res: any)=>{
    this.enrollmentPercent = res.metricValueList[0];
    let xvalues =[];
    let yvalues =[];
    for(let i=0;i<12;i++){
      xvalues[i] = res.periodValueList[i]
    }
    for(let i=12,j=0;i<24;i++,j++){
      yvalues[j] = res.periodValueList[i]
    }
    this.enrollmentChart = {
      labels: ['Y','N'],
      datasets: [
          {
              data: [res.metricValueList[0],100-res.metricValueList[0]],
              backgroundColor: [
                  "#1F7FFF",
                  "#E4E5E6"
                 
              ],
              hoverBackgroundColor: [
                  "#1F7FFF",
                  "#E4E5E6"
              ]
          }]    
      };
      this.enrollmentChart1 = {
        labels: ['January', 'February', 'March', 'April', 'May', 'June', 'July','August','September','October','November','December'],
        datasets: [
            {
              label: '2019',
              lineTension: 0,        
                data: xvalues,
                fill: false,
                borderColor: '#4bc0c0'
            },
            {
                label: '2018',
                data: yvalues,
                fill: false,
                borderColor: '#565656'
            }
        ]
    }
    this.enrollmentyoy = res.yoyList[0];
  });
  this.GapsService.getObjectiveClaims().subscribe((res: any)=>{
    this.claimsPercent = res.metricValueList[0];
    let xvalues =[];
    let yvalues =[];
    for(let i=0;i<12;i++){
      xvalues[i] = res.periodValueList[i]
    }
    for(let i=12,j=0;i<24;i++,j++){
      yvalues[j] = res.periodValueList[i]
    }
   // console.log(xvalues)
    this.cliamsChart = {
      labels: ['Y','N'],
      datasets: [
          {
              data: [res.metricValueList[0],100-res.metricValueList[0]],
              backgroundColor: [
                "#FDBA03",
                "#E4E5E6"
               
            ],
            hoverBackgroundColor: [
                "#FDBA03",
                "#E4E5E6"
            ]
          }]    
      };
      this.cliamsChart1 = {
        labels: ['January', 'February', 'March', 'April', 'May', 'June', 'July','August','September','October','November','December'],
        datasets: [
            {
              label: '2019',  
                data: xvalues,
                fill: false,
                borderColor: '#4bc0c0'
            },
            {
                label: '2018',     
                data: yvalues,
                fill: false,
                borderColor: '#565656'
            }
        ]
    }
    this.cliamsyoy = res.yoyList[0];
  });
  this.GapsService.getObjectiveMemberHealth().subscribe((res: any)=>{
    this.memberHealthPercent = res.metricValueList[0];
    let xvalues =[];
    let yvalues =[];
    for(let i=0;i<12;i++){
      xvalues[i] = res.periodValueList[i]
    }
    for(let i=12,j=0;i<24;i++,j++){
      yvalues[j] = res.periodValueList[i]
    }
   // console.log(xvalues)
    this.memberHealthChart = {
      labels: ['Y','N'],
      datasets: [
          {
              data: [res.metricValueList[0],100-res.metricValueList[0]],
              backgroundColor: [
                "#B47DFB",
                "#E4E5E6"
               
            ],
            hoverBackgroundColor: [
                "#B47DFB",
                "#E4E5E6"
            ]
          }]    
      };
      this.memberHealthChart1 = {
        labels: ['January', 'February', 'March', 'April', 'May', 'June', 'July','August','September','October','November','December'],
        datasets: [
            {
              label: '2019',
                data:xvalues,
                fill: false,
                borderColor: '#4bc0c0'
            },
            {
                label: '2018',
                data: yvalues,
                fill: false,
                borderColor: '#565656'
            }
        ]
    }
    this.memberHealthyoy  = res.yoyList[0];
  });
  this.GapsService.getObjectiveBrandLoyalty().subscribe((res: any)=>{
    this.brandLoyaltyPercent = res.metricValueList[0];
    let xvalues =[];
    let yvalues =[];
    for(let i=0;i<12;i++){
      xvalues[i] = res.periodValueList[i]
    }
    for(let i=12,j=0;i<24;i++,j++){
      yvalues[j] = res.periodValueList[i]
    }
    this.brandChart = {
      labels: ['Y','N'],
      datasets: [
          {
              data: [res.metricValueList[0],100-res.metricValueList[0]],
              backgroundColor: [
                "#03C960",
                "#E4E5E6"
               
            ],
            hoverBackgroundColor: [
                "#03C960",
                "#E4E5E6"
            ]
          }]    
      };
      this.brandChart1 = {
        labels: ['January', 'February', 'March', 'April', 'May', 'June', 'July','August','September','October','November','December'],
        datasets: [
            {
              label: '2019',
                data: xvalues,
                fill: false,
                borderColor: '#4bc0c0'
            },
            {
                label: '2018',
                data: yvalues,
                fill: false,
                borderColor: '#565656'
            }
        ]
    }
    this.brandyoy = res.yoyList[0];
  });
  
  
  }

}
