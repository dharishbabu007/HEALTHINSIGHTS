import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
@Component({
    selector: 'app-view-persona',
    templateUrl: './view-persona.component.html',
    styleUrls: ['./view-persona.component.scss']
})
export class ViewPersonaComponent implements OnInit {
    public externalURL: any;
    value = 50;
    personaList: any;
    constructor(private route: ActivatedRoute) {
    }
    
    ngOnInit() {
     this.personaList = [
        // {
        //   label: 'Lazy Doer',
        //   value: '1'
        // },
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

    }
}
 