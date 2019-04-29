import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
import { GapsService } from '../../shared/services/gaps.service';
import { Router } from '@angular/router';

@Component({
    selector: 'app-pc-stats',
    templateUrl: './pc-stats.component.html', 
     styleUrls: ['./pc-stats.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class PCStatsComponent implements OnInit {
    constructor(private gapsService: GapsService, private route: ActivatedRoute, public router: Router) {
    }
    DashboardClick(){
 
    }
    clusterData: any[];
    clusterfeatures:any;
    cols: any[];
    cols1:any[];
    field:any;
    mergeCols:any;
    ngOnInit() {
                // this.gapsService.getpersonaClusteringstats().subscribe((data: any[]) => {
                //     this.clusterData = data;
                // });
                // this.cols = [
                //     { field: 'patientId', header: 'Patient Id' },
                //     { field: 'patientName', header: 'Patient Name' },          
                //     { field: 'appointmentID', header: 'Appointment ID' },
                //     { field: 'appointmentDay', header: 'Appointment Day' },
                //     { field: 'neighbourhood', header: 'Neighbourhood' },
                //     { field: 'likelihood', header: 'Likelihood' },
                //     { field: 'noShow', header: 'No-Show' }
                // ];
                this.gapsService.getpersonaClusteringstats().subscribe((data: any[]) => {
                    this.clusterData = data;
                    this.field = [];
                    this.field.push(this.clusterData[0]);
                    this.field.push(this.clusterData[1]);
                    this.field.push(this.clusterData[2]);
                    this.mergeCols = [];
                    this.mergeCols.push(this.clusterData[3]);
                    this.mergeCols.push(this.clusterData[4]);
                    this.mergeCols.push(this.clusterData[5]);
                    
                      // this.field = this.membergaps2.length-1;
                            // for(let i=0;i<this.membergaps2.length-1;i++){
                            //     for(let j=0;j<this.membergaps2.length-1;j++){
                            //     console.log(this.membergaps2[0][i])
                            //     this.cols1.push({field: this.membergaps2[i+1][j],header:this.membergaps2[0][i]})
                            //     }
                            // }
                  this.cols1 = this.field.filter(item => item);
                  //console.log(this.cols1)
                });
                this.gapsService.getpersonaClusteringfeatures().subscribe((data: any[]) => {
                    this.clusterfeatures = data;
                });
                this.cols1 = [
                    { field: 'clusterId', header: 'Cluster Id' },
                    { field: 'featureName', header: 'Feature Name' },          
                    { field: 'featureType', header: 'Feature Type' },
                    // { field: 'appointmentDay', header: 'Appointment Day' },
                    { field: 'maxFrequency', header: 'Max Frequency' },
                    { field: 'featureSignificanceValue', header: 'Feature Significance Value' },
                    { field: 'x', header: 'X' },
                    { field: 'y', header: 'Y' }
                ];
            
        
    }
}
