import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';
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
    cols1: any[];
    cols2: any[];
    cols3 : any[];
    ngOnInit() {
      this.gapsService.getClusterStatistics().subscribe((data: any[]) => {
            this.membergaps = data;
           // console.log(data);
            this.cols = this.membergaps.filter(item => item);
            this.cols1 =[]
            this.cols2 =[]
            this.cols3 = []
           // console.log(this.cols)
           this.cols.forEach(ele =>
           {
            this.cols1.push(ele[0]);
           })
           this.cols.forEach(ele =>
            {
             this.cols2.push(ele[1]);
             this.cols3.push(ele[2])
           })
           
       // console.log(this.cols2)
     });      
    }
}
