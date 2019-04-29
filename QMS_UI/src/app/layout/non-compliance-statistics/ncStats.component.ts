import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';
@Component({
    selector: 'app-ncStats',
    templateUrl: './ncStats.component.html', 
    styleUrls: ['./ncStats.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class NcStatsComponent implements OnInit {
    data: any;
    tp:any;
    fp:any;
    tn:any;
    fn:any;
    ModelScore: any[];
    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router) {
        this.route.params.subscribe(params => {
            
        });
    }
    membergaps: MemberCareGaps[];
    cols: any[];
    ngOnInit() {

        // this.sales = [
        //     {brand:'Positive',num:'34999',num2: '332'},
        //     {brand:'Negative',num:'8753',num2: '126'}
        // ];
        this.gapsService.getncModelMetric().subscribe((data:any)=>{
           // console.log(data)
            this.ModelScore = data.score;
            this.tp= data.tp;
            this.tn =data.tn;
            this.fp = data.fp;
            this.fn = data.fn;
        })
        
        this.gapsService.getncModelSummary().subscribe((data: MemberCareGaps[]) => {
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
