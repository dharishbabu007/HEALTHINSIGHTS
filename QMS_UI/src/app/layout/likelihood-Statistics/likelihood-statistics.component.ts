import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';
@Component({
    selector: 'app-tables',
    templateUrl: './likelihood-statistics.component.html', 
    styleUrls: ['./likelihood-statistics.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class LikelihoodStatisticsComponent implements OnInit {
    sales: any[];
    data: any;

    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router) {
        this.route.params.subscribe(params => {
            
        });
    }
    membergaps: MemberCareGaps[];
    confusionmatrix: MemberCareGaps[];
    modelscore: any;
    cols: any[];
    ModelScore: any[];
    ConfusionMatrix: any[];
    imageUrl: any;
    ngOnInit() {

        this.gapsService.getlikelihoodmodelScore().subscribe((data: any) =>{
                this.modelscore = data;
                this.ModelScore = [
                    {num:this.modelscore.score},    
               ];
               this.imageUrl = this.modelscore.imageFullPath;

        });
       
       // this.imageUrl = this.modelscore[0].imageFullPath;
        this.gapsService.getlikelihoodconfusionmatric().subscribe((data: MemberCareGaps[]) => {
            this.confusionmatrix = data;
        });

        this.ConfusionMatrix = [
            { field: 'id', header: 'ID' },
            { field: 'zero', header: 'Zero' },
            { field: 'one', header: 'One' },
        ];

        this.gapsService.getlikelihoodmodel().subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        });
        this.cols = [
            { field: 'attributes', header: 'Attributes' },
            { field: 'estimate', header: 'Estimate' },
            { field: 'stdError', header: 'Std Error' },
            { field: 'zValue', header: 'Z-value' },
            { field: 'prz', header: 'P-value' },
           // {field: 'significance', header: 's'}
        ];
       
        this.data = {
            labels: ['January', 'February', 'March', 'April', 'May', 'June', 'July'],
            datasets: [
                {
                    label: 'First Dataset',
                    data: [65, 59, 80, 81, 56, 55, 40]
                }
            ]
        }
    }
}
