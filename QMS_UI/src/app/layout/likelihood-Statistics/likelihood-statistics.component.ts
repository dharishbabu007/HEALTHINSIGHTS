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
    membergapslhe:any;
    membergapslhc:any;
    tp:any;
    fp:any;
    tn:any;
    fn:any;
    ModelScorelhe: any;
    tplhe:any;
    fplhe:any;
    tnlhe:any;
    fnlhe:any;
    mergeCols:any;
    ngOnInit() {
        if(this.type == 1){
        this.gapsService.getlhcmodelsummary().subscribe((data: MemberCareGaps[]) => {
            this.membergapslhc = data;
          //  console.log(data)
        });
        this.cols = [
            { field: 'attributes', header: 'Attributes' },
            { field: 'estimate', header: 'Estimate' },
            { field: 'stdError', header: 'Std Error' },
            { field: 'zValue', header: 'Z-value' },
            { field: 'prz', header: 'P-value' },
           // {field: 'significance', header: 's'}
        ];
        this.gapsService.getLhcModelMetric().subscribe((data:any)=>{
            // console.log(data)
             this.ModelScore = data.score;
             this.tp= data.tp;
             this.tn =data.tn;
             this.fp = data.fp;
             this.fn = data.fn;
         });

    }
    else{
        this.gapsService.getLHEModelSummary().subscribe((data: MemberCareGaps[]) => {
            this.membergapslhe = data;
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
        this.gapsService.getLheModelMetric().subscribe((data: any) => {
                        // console.log(data)
                        this.ModelScorelhe = data.score;
                        this.tplhe= data.tp;
                        this.tnlhe =data.tn;
                        this.fplhe = data.fp;
                        this.fnlhe = data.fn;

        });
                         
        this.gapsService.getLikelihoodClusterStatistics().subscribe((data: MemberCareGaps[]) => {
            this.membergaps2 = data;
            this.loading = false;
            this.field = [];
            this.field.push(this.membergaps2[0]);
            this.field.push(this.membergaps2[1]);
            this.field.push(this.membergaps2[2]);
            this.mergeCols = [];
            this.mergeCols.push(this.membergaps2[3]);
            this.mergeCols.push(this.membergaps2[4]);
            this.mergeCols.push(this.membergaps2[5]);
            
              // this.field = this.membergaps2.length-1;
                    // for(let i=0;i<this.membergaps2.length-1;i++){
                    //     for(let j=0;j<this.membergaps2.length-1;j++){
                    //     console.log(this.membergaps2[0][i])
                    //     this.cols1.push({field: this.membergaps2[i+1][j],header:this.membergaps2[0][i]})
                    //     }
                    // }
          this.cols1 = this.field.filter(item => item);
        });

        
        
    }   
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
