import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';

@Component({
    selector: 'app-clustermemberlist',
    templateUrl: './cluster-memberlist.component.html',
    styleUrls: ['./cluster-memberlist.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class ClusterMemberListComponent implements OnInit {
    constructor(private gapsService: GapsService) {}
    membergaps: MemberCareGaps[];
    cols: any[];
    loading = true;
    personas =  [
        { label: 'Persona1', value: 'persona1' },
        { label: 'Persona2', value: 'persona2' }
    ];
    ngOnInit() {
        this.gapsService.getMeasureDetails().subscribe((data: MemberCareGaps[]) => {
            data.forEach(element => {
                element.age = parseInt(element.age, 10);
            });
            this.membergaps = data;
            this.loading = false;
        });
        this.cols = [
            { field: 'id', header: 'Member Id' },
            { field: 'name', header: 'Name' },
            { field: 'age', header: 'Age' },
            { field: 'gender', header: 'Gender' },
            
            { field: 'goals', header: 'Goals' },
            { field: 'motivations', header: 'Motivations' },
            { field: 'channel', header: 'Preffered Communication Channel' },
        ];
    }
}
