import { Component, OnInit } from '@angular/core';
import { GapsService } from '../../shared/services/gaps.service';
import { FormGroup, FormControl, Validators, FormBuilder} from '@angular/forms';
import { Router } from '@angular/router';

import { ActivatedRoute } from '@angular/router';
import { MessageService } from '../../shared/services/message.service';
import {TreeNode} from 'primeng/api';

@Component({ 
    selector: 'app-tables',
    templateUrl: './create-role.component.html', 
     styleUrls: ['./create-role.component.scss'],
    providers: [GapsService]
})
export class CreateRoleComponent implements OnInit {

    files:any[];
    public myForm: FormGroup;
    selectedFiles2: any[];
    measureId: string;
    roleList: any[];
    constructor(private _fb: FormBuilder,
      private GapsService: GapsService,
      private router: Router,
      private route: ActivatedRoute,
    private msgService: MessageService) {
     
      this.route.params.subscribe(params => {
              this.measureId = params['measureId'];
          }); }
  
    ngOnInit() {

        this.myForm = this._fb.group({
            RoleName: [''],
            'cities': new FormControl(),
        
          });


          this.files = [
            {
                "label": "Quality Management",
                "data": "QualityManagement",
               
                "children": [{
                        "label": "Quality Central",
                        "data": "QualityCentral",
                     
                        "children": [{"label": "Read", "data": "Read"}, {"label": "Write", "data": "Write"}]
                    },
                    {
                        "label": "Quality Central",
                        "data": "QualityMeasures",
                   
                        "children": [{"label": "Read", "data": "Read"}, {"label": "Write", "data": "Write"}]
                    }]
            },
            {
                "label": "Gaps in Care",
                "data": "Gaps in Care",
                "children": [{
                    "label": "Gaps Registry",
                    "data": "GapsRegistry",
                 
                    "children": [{"label": "Read", "data": "Read"}, {"label": "Write", "data": "Write"}]
                },
                {
                    "label": "Close Gap Patient",
                    "data": "CloseGapPatient",
               
                    "children": [{"label": "Read", "data": "Read"}, {"label": "Write", "data": "Write"}]
                }]
            },
            {
                "label": "PHM",
                "data": "PHM",
                "children": [{
                    "label": "PHM Summary",
                    "data": "PHMSummary",
                 
                    "children": [{"label": "Read", "data": "Read"}, {"label": "Write", "data": "Write"}]
                },
                {
                    "label": "Risk Assessment",
                    "data": "RiskAssessment",
               
                    "children": [{"label": "Read", "data": "Read"}, {"label": "Write", "data": "Write"}]
                }]
            },
            {
                "label": "Analytics Workbench",
                "data": "AnalyticsWorkbench",
             
                "children": [{
                    "label": "Development Studio",
                    "data": "DevelopmentStudio",
                 
                    "children": [{"label": "Read", "data": "Read"}, {"label": "Write", "data": "Write"}]
                },
                {
                    "label": "Use Cases",
                    "data": "UseCases",
               
                    "children": [{"label": "Read", "data": "Read"}, {"label": "Write", "data": "Write"}]
                }]
            },
            {
                "label": "User Management",
                "data": "UserManagement",
             
                "children": [{
                    "label": "Role Creation",
                    "data": "RoleCreation",
                 
                    "children": [{"label": "Read", "data": "Read"}, {"label": "Write", "data": "Write"}]
                },
                {
                    "label": "User Creation",
                    "data": "UserCreation",
               
                    "children": [{"label": "Read", "data": "Read"}, {"label": "Write", "data": "Write"}]
                }]
            }
        ]


    }
    onSubmit(){
        
    }
}
