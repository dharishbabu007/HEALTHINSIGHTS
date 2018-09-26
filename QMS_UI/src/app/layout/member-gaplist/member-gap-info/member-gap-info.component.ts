import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { routerTransition } from '../../../router.animations';
import { Gaps, CloseGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';
import { MessageService } from '../../../shared/services/message.service';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { DatePipe } from '@angular/common';

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
    i: any;
    j:any;
    gapId: string;
    dateFlag : any;
    memberId: number;
    measureTitle: any;
    now = new Date();
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
           
           console.log(gapsArray)
            if (gapsArray.length) {
                this.gaps = gapsArray;
                //console.log(gapsArray);
               // this.closeGapForm.controls['priority'].setValue(gapsArray[0].priority);
               // this.closeGapForm.controls['status'].setValue(gapsArray[0].status);
                this.closeGapForm.controls['intervention'].setValue(gapsArray[0].intervention);
                this.closeGapForm.controls['payerComments'].setValue(gapsArray[0].payerComments);
               // this.closeGapForm.controls['providerComments'].setValue(gapsArray[0].providerComments);
            }
            this.i = 0;
        for(let num of this.closeGaps.careGaps){
          
            if(num.status === "Close by Payer"){
                this.flag = 1;
               this.j=this.i+1;
               
               if(this.closeGaps.careGaps[this.j].status == "Open"){
                //console.log(this.closeGaps.careGaps[this.j].status)
                this.flag = 0;
               // console.log(this.flag);
               }
            }
          this.i= this.i +1;
        }
       this.measureTitle = this.gaps[0].measureTitle;
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
    formatDate(dateString) {
        if (dateString) {
          const datePipe = new DatePipe('en-US');
          return datePipe.transform(dateString, 'yyyy-MM-dd 00:00:00');
        } else {
          return null;
        }
      } 
    onSubmit(formModel) {
        for(let num of this.closeGaps.careGaps){
          if(num.status === formModel.status && this.formatDate(num.dateTime) === this.formatDate(this.now)){
                this.dateFlag = 1;
              // console.log(num.dateTime);
            }
        }
        console.log(this.dateFlag);
        if(this.dateFlag !=1){
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
          else{
              this.msgService.success("The record already has a same status at same date")
          }
        }
       
        

}


