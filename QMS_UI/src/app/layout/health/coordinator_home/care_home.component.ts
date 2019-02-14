import { Component, OnInit } from '@angular/core';
import {GapsService }from '../../../shared/services/gaps.service';

@Component({
  selector: 'app-care_home',
  templateUrl: './care_home.component.html',
  styleUrls: ['./care_home.component.scss']
})
export class CoordinatorHomeComponent implements OnInit {
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

  constructor(private GapsService:GapsService) { 
  }

  ngOnInit() {
    var user = JSON.parse(localStorage.getItem('currentUser'));
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

}
}
