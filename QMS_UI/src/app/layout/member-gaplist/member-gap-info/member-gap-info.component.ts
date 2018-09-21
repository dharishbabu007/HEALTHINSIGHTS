import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { routerTransition } from '../../../router.animations';
import { Gaps, CloseGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';
import { MessageService } from '../../../shared/services/message.service';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';

@Component({
    selector: 'app-tables',
    templateUrl: './member-gap-info.component.html',
    styleUrls: ['./member-gap-info.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class MemberGapInfoComponent implements OnInit {
    closeGaps: CloseGaps;
    gaps: any;
    cols: any[];
    flag: any;
    gapId: string;
    i : any;
    memberId: number;
    statusTypes =  [
        {label: 'Select Status', value: null},
        { label: 'Open', value: 'Open' },
        { label: 'Open with Provider', value: 'Open with Provider' },
        { label: 'Close by Provider', value: 'Close by Provider' },
        { label: 'Open with Payer', value: 'Open with Payer' },
        { label: 'Close by Payer', value: 'Close by Payer' }
    ];
    priorityTypes =  [{label: 'Select Priority', value: null},{ label: 'High', value: 'High' }, { label: 'Low', value: 'Low' }, { label: 'Medium', value: 'Medium' }];
    public closeGapForm: FormGroup;
    constructor(private gapsService: GapsService,
        private msgService: MessageService,
        private _fb: FormBuilder,
        private router: Router,
        private route: ActivatedRoute) {
            this.route.params.subscribe(params => {
                this.gapId = params['gapId'];
                this.memberId = params['memberId'];
            });
            this.closeGapForm = this._fb.group({
                priority: [],
                status: [],
                intervention: [],
                payerComments: [],
                providerComments: []
            }); 
        }

    ngOnInit() {
        this.gapsService.getGapsInfo(this.gapId, this.memberId).subscribe((data: CloseGaps) => {
            this.closeGaps = data;

            const gapsArray = this.closeGaps.careGaps;
            gapsArray[0].priority = null;
            gapsArray[0].status = null;
            console.log(gapsArray)
            if (gapsArray.length) {
                this.gaps = gapsArray;
                console.log(gapsArray);
                this.closeGapForm.controls['priority'].setValue(gapsArray[0].priority);
                this.closeGapForm.controls['status'].setValue(gapsArray[0].status);
                this.closeGapForm.controls['intervention'].setValue(gapsArray[0].intervention);
                this.closeGapForm.controls['payerComments'].setValue(gapsArray[0].payerComments);
                this.closeGapForm.controls['providerComments'].setValue(gapsArray[0].providerComments);
            }
            
        for(let num of this.closeGaps.careGaps){
           
            if(num.status === "Close by Payer"){
                this.flag = 1;
                console.log(num.status);
            }
        }

        });
        this.cols = [
            { field: 'measureTitle', header: 'Care Gaps' },
            { field: 'intervention', header: 'Interventions' },
            { field: 'priority', header: 'Priority' },
           // { field: 'payerComments', header: 'Payer Comments' },
            { field: 'providerComments', header: 'Provider Comments' },
            { field: 'status', header: 'Status' },
            { field: 'dateTime', header: 'Last Action Taken' },
        ];
       

    }
    onSubmit(formModel) {
        
       
        if( this.flag == 1 && formModel.status!= 'Open'){
         this.msgService.error("already closed by payer you can only change it to open");
        }

          else{
            console.log("came here too ")
            this.gapsService.updateCloseGaps(formModel, this.memberId, this.gapId).subscribe((res: any) => {
                if (res.status === 'SUCCESS') {
                    this.msgService.success('Closegap updated Successfully');
                    // this.router.navigateByUrl('/measureworklist?fetch');
                  } else {
                    this.msgService.error(res.message);
                  }
            }); 
          }
    
          }
        

}


