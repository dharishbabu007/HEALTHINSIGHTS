import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { GapsService } from '../../../shared/services/gaps.service';
@Component({
    selector: 'app-view-persona',
    templateUrl: './view-persona.component.html',
    styleUrls: ['./view-persona.component.scss']
})
export class ViewPersonaComponent implements OnInit {
    public externalURL: any;
    value = 50;
    personaList: any;
    clusterId: any;
    clusterData: any;
    motivations:any;
    constructor(private route: ActivatedRoute,private GapsService:GapsService) {
        this.route.params.subscribe(params => {
            this.clusterId = params['clusterId'];
            
        });
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
        if(this.clusterId){
            this.GapsService.getClusterFormData(this.clusterId).then((data : any)=>{
                this.clusterData = data.clusterPersona;
               // console.log(data)

            });
            
            this.GapsService.getgraphdata(this.clusterId ,"What are your motivations for leading a healthy life?").subscribe((res:any)=>{
            //  console.log(res);
              this.motivations = [];
              this.motivations = res;
            //   console.log(res)
            })
        }

    }

}
 