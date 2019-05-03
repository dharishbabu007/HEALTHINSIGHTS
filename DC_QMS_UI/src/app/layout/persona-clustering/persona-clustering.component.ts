import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';

@Component({
    selector: 'app-persona-clustering',
    templateUrl: './persona-clustering.component.html', 
     styleUrls: ['./persona-clustering.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class PersonaClusteringComponent implements OnInit {
modelName:any;
title:any;
loading = true;
    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router) {
    }
    DashboardClick(){
 
    }
    clusteringOutput: any[];
    cols: any[];
    ngOnInit() {
                this.gapsService.getpersonaClustering().subscribe((data: any[]) => {
                    this.loading= false;
                    this.clusteringOutput = data;
                });
                this.cols = [
                    { field: 'memberId', header: 'Member Id' },
                    { field: 'memberName', header: 'Member Name' },          
                    { field: 'age', header: 'Age' },
                    { field: 'daysPendingTermination', header: 'Days Pending for Termination' },
                    { field: 'outPocketExpenses', header: 'Out Pocket Expenses' },
                    { field: 'formExercise', header: 'Form of Exercise' },
                    { field: 'frequencyExercise', header: 'Frequency of Exercise' },
                    { field: 'measureCalorieIntake', header: 'Measure calorie intake' },
                    { field: 'dailyConsumption', header: 'Daily consumption' },
                    { field: 'weight', header: 'Weight' },
                    { field: 'motivations', header: 'Motivations' },
                    { field: 'amountSpend', header: 'Amount Spend' },
                    { field: 'clusterId', header: 'Cluster Id' },

                ];
            
        
    }
}
