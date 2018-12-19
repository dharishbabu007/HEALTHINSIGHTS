import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { ActivatedRoute } from '@angular/router';
@Component({
    selector: 'app-tables',
    templateUrl: './persona-data.component.html',
    styleUrls: ['./persona-data.component.scss'],
    animations: [routerTransition()]
})
export class PersonaDataComponent implements OnInit {
    public externalURL: any;
    constructor(private route: ActivatedRoute) {
    }
    personaList = [
        {
          label: 'Lazy Doer',
          value: '1'
        },
        {
          label: 'Champions',
          value: '2'
        },
        {
          label: 'Informed Achievers',
          value: '3'
        },
        {
          label: 'Trend Setters',
          value: '4'
        },
        {
          label: 'Self Neglecters',
          value: '5'
        },
        {
            label: 'Number Crunchers',
            value: '6'
        },
        {
            label: 'Problem Seekers',
            value: '7'
        }
        
        ];

      
    ngOnInit() {
   
    }
}
 