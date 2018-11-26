import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';
import { NgxPermissionsService, NgxRolesService} from 'ngx-permissions';
@Component({
    selector: 'app-clusterStats',
    templateUrl: './cluster-statistics.component.html',
    styleUrls: ['./cluster-statistics.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class ClusterStatisticsComponent implements OnInit {
     
    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router,  private permissionsService: NgxPermissionsService,) {
        this.route.params.subscribe(params => {
     
        });
    }
    membergaps: MemberCareGaps[];
    cols: any[];
    ngOnInit() {
      this.gapsService.getClusterStatistics().subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        }); 
        this.cols = [
            { field: 'aggStage', header: 'Agglomeration stage' },
            { field: 'test1', header: 'Test 1' },
            { field: 'test2', header: 'Test 2' },
            { field: 'test3', header: 'Test 3' },
            { field: 'test4', header: 'Test 4' },
            { field: 'test5', header: 'Test 5' },
            { field: 'test6', header: 'Test 6'}
        ];
    }
}
