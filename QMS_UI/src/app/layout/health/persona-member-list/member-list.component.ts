import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../../router.animations';
import { MemberCareGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';
import { ActivatedRoute } from '@angular/router';

@Component({
    selector: 'app-tables',
    templateUrl: './member-list.component.html',
    styleUrls: ['./member-list.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class PersonaMemberListComponent implements OnInit {
    type: any;
    constructor(private gapsService: GapsService,private route: ActivatedRoute) {
        this.route.params.subscribe(param=>
        this.type = param['type']
    )
    }
    membergaps: MemberCareGaps[];
    membergaps1: MemberCareGaps[];
    cols: any[];
    cols1:any[];
    selectionName:any;
    selectList =[{label:'Cluster',value:'cluster'},{label:'Persona',value:'persona'}]
    nameList: any[];
    loading = true;
     genderTypes =  [
        { label: 'Select', value: '' },
        { label: 'Male', value: 'Male' },
        { label: 'Female', value: 'Female' }
    ];
    ngOnInit() {
        this.cols = [
            { field: 'memberId', header: 'Member ID' },
            { field: 'personaName', header: 'Persona Name' },
            { field: 'age', header: 'Age' },
            { field: 'gender', header: 'Gender' },
            { field: 'modeOfContact', header: 'How do you like to be contacted?' },
            { field: 'formOfExercise', header: 'What form of excercise do you prefer?' },
            { field: 'idealHealthGoal', header: 'What is your ideal goal towards your health?' },
            { field: 'socialMediaUsage', header: 'Do you actively use social media?' } 
        ];

    }
    SelectionList(event){
        this.selectionName = event.value;
        if(event.value=='cluster'){
            this.gapsService.getclusterlist().subscribe((res: any) =>{
                this.nameList =[];
               // console.log(res)
                res.forEach(item => {
                    this.nameList.push({label: "cluster "+item,value: item});
                });
            });
        }
        else{
            this.gapsService.getPersonaNames().subscribe((res:any)=>{
                this.nameList=[];
                res.forEach(element =>{
                    this.nameList.push({label:element,value:element})
                })
            })
        }
    }
    nameSelection(event){
        this.gapsService.getPersonaMemberList(this.selectionName,event.value).subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        });
        this.cols = [
            { field: 'memberId', header: 'Member ID' },
            { field: 'personaName', header: 'Persona Name' },
            { field: 'age', header: 'Age' },
            { field: 'gender', header: 'Gender' },
            { field: 'modeOfContact', header: 'How do you like to be contacted?' },
            { field: 'formOfExercise', header: 'What form of excercise do you prefer?' },
            { field: 'idealHealthGoal', header: 'What is your ideal goal towards your health?' },
            { field: 'socialMediaUsage', header: 'Do you actively use social media?' } 
        ];

    }
}
