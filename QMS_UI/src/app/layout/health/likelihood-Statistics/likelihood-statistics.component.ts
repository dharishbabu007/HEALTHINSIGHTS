import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';
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
    type: any;
    title: any;
    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router) {
        this.route.params.subscribe(params => {
            this.type = params['type'];
            this.title = (this.type === '1' ) ? 'Likelihood To Churn Statistics' : 'Statistics - Likelihood To Enroll ';
            
        });
    }
    membergaps: MemberCareGaps[];
    confusionmatrix: MemberCareGaps[];
    modelscore: any;
    cols: any[];
    LHEModelSummary: any[];
    LHEModelMatric: any[];
    ModelScore: any[];
    ConfusionMatrix: any[];
    imageUrl: any;
    membergaps1: any;
    membergaps2: any;
    cols1: any =[];
    field: any;
    header: any;
    loading: boolean = true;
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
       // console.log(this.ConfusionMatrix)

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
        this.gapsService.getLHEModelSummary().subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        });
        this.LHEModelSummary = [
            { field: 'attributes', header: 'Attributes' },
            { field: 'estimate', header: 'Estimate' },
            { field: 'stdError', header: 'Std Error' },
            { field: 'zValue', header: 'Z-value' },
            { field: 'prz', header: 'P-value' },
           // {field: 'significance', header: 's'}
        ];
       // console.log(this.LHEModelSummary)
        this.gapsService.getLHEModelMatric().subscribe((data: MemberCareGaps[]) => {
            this.membergaps1 = data;
            this.LHEModelMatric = [
                { field: this.membergaps1.tp , header: 'tp' },
                { field: this.membergaps1.fp, header: 'fp' },
                { field: this.membergaps1.tn, header: 'tn' },
                { field: this.membergaps1.fn, header: 'fn' },
                { field: this.membergaps1.score, header: 'Score' },
               // {field: 'significance', header: 's'}
            ];
         //   console.log(this.LHEModelMatric)
        });
        this.gapsService.getLikelihoodClusterStatistics().subscribe((data: MemberCareGaps[]) => {
            this.membergaps2 = data;
            this.loading = false;
            this.field = this.membergaps2.length-1;
            // for(let i=0;i<this.membergaps2.length-1;i++){
            //     for(let j=0;j<this.membergaps2.length-1;j++){
            //     console.log(this.membergaps2[0][i])
            //     this.cols1.push({field: this.membergaps2[i+1][j],header:this.membergaps2[0][i]})
            //     }
            // }
          this.cols1 = this.membergaps2.filter(item => item);
          //console.log(this.cols1)
        });
        
        
        
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
