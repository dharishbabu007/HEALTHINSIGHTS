import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';
@Component({
    selector: 'app-tables',
    templateUrl: './csv2.component.html', 
    styleUrls: ['./csv2.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class Csv2Component implements OnInit {
    sales: any[];
    data: any;
    
    ModelScore: any[];
    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router) {
        this.route.params.subscribe(params => {
            
        });
    }
    membergaps: MemberCareGaps[];
    cols: any[];
    ngOnInit() {

        this.sales = [
            {brand:'Positive',num:'35330',num2: '8878'},
            {brand:'Negative',num:'1',num2: '1'}
        ];

        this.ModelScore = [
             {num:'63987'},
             
        ];
        this.gapsService.getCsv2().subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        });
        this.cols = [
            { field: 'attributes', header: 'Attributes' },
            { field: 'estimate', header: 'Estimate' },
            { field: 'stdError', header: 'Std Error' },
            { field: 'zValue', header: 'Z-value' },
            { field: 'prz', header: 'P-value' },
            {field: 'significance', header: 's'}
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