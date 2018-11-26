import { Component, OnInit } from '@angular/core';
import { GapsService } from '../../shared/services/gaps.service';
import {MenuItem} from 'primeng/api';
@Component({
    selector: 'app-member-engage',
    templateUrl: './member-engage.component.html',
    styleUrls: ['./member-engage.component.scss'],
    providers: [GapsService],
})

export class MemberEngageComponent implements OnInit {
    programs: any;
    clinicalConditions: any;
    measureDomains: any;
    memberList: any;
    constructor(private gapsService: GapsService) {
    }
    items: MenuItem[];
    showMainContent: Boolean = true;
    showMainContent1: Boolean = true;
    showMainContent2: Boolean = true;
    ngOnInit() {
        this.gapsService.getPrograms().subscribe((data: any) => {
            this.programs = data;
        });
        this.gapsService.getClinicalConditions().subscribe((data: any) => {
            this.clinicalConditions = data;
        });
        this.gapsService.getMeasureDomain().subscribe((data: any) => {
            this.measureDomains = data;
        });
        this.items = [
            {
                label: 'Clustering',
            },
            {
                label: 'Personas',
              
                items: [
                    {label: 'Create Persona'},
                    {label: 'View Persona', routerLink: ['/csv2'], routerLinkActiveOptions: { exact: true } }
                ]
            }
        ];
     
    }
    ShowHideButton(){
        this.showMainContent = this.showMainContent ? false : true ;

    }
    ShowHideButton1(){
        this.showMainContent1 = this.showMainContent1 ? false : true ;

    }
    ShowHideButton2(){
        this.showMainContent2 = this.showMainContent2 ? false : true ;

    }
}
    
