import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { routerTransition } from '../../../router.animations';
import { Gaps, CloseGaps } from '../../../shared/services/gaps.data';
import { GapsService } from '../../../shared/services/gaps.service';
import { MessageService } from '../../../shared/services/message.service';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { DatePipe } from '@angular/common';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { throwError } from 'rxjs';
import { MemberGap } from './member-gap-info.component';
import { MemberGapService } from './member-gap-info.service';

@Component({
    selector: 'app-tables',
    templateUrl: './member-gap-info.component.html',
    styleUrls: ['./member-gap-info.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})

export class MemberGapInfoComponent implements OnInit {
    closeGaps: any;
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
    SelectedFile: File = null;
    targetDate: any;
    fd: any;
    dowloadData:any;
    closeGapCreated:boolean = false;
    fileUploadStatus:boolean = false;
    headers={
        headers: new HttpHeaders({
          'Content-Type':  'application/json',
          'Authorization': 'my-auth-token',
          'Access-Control-Allow-Origin': '*',
        })
       }

    statusTypes =  [
        {label: 'Select Status', value: null},
        { label: 'Open', value: 'Open' },
        { label: 'Open with Provider', value: 'Open with Provider' },
        { label: 'Close by Provider', value: 'Close by Provider' },
        { label: 'Open with Payer', value: 'Open with Payer' },
        { label: 'Close by Payer', value: 'Close by Payer' }
    ];
    actionTypes= [
        {label: 'Select Action', value: null},
        { label: 'Exclusion', value: 'Exclusion' },
        { label: 'Scheduled', value: 'Scheduled' },
        { label: 'Completed', value: 'Completed' },
        { label: 'MIT', value: 'MIT' },
        { label: 'Other', value: 'Other' }
    ];
    priorityTypes =  [{ label: 'Low', value: 'Low' }, { label: 'Medium', value: 'Medium' },{ label: 'High', value: 'High' }];
    public closeGapForm: FormGroup;
    roleList: any[];
    roleListRepositry:any;
    rolename: any;
    provider: boolean = false;
    firstrow: any;
    remainingrows: any;
    constructor(private gapsService: GapsService,
        private msgService: MessageService,
        private _fb: FormBuilder,
        private router: Router,
        private route: ActivatedRoute,
    public MemberGapService :MemberGapService ) {
       
            this.route.params.subscribe(params => {
                this.gapId = params['gapId'];
                this.memberId = params['memberId'];
            });
            this.closeGapForm = this._fb.group({
                priority: [],
                status: [],
                intervention: [],
               payerComments: [],
                providerComments: [],
                closeGap:[],
                actionOnCareGap: []
            }); 
           
        }

    ngOnInit() {
        var user =  JSON.parse(localStorage.getItem('currentUser'));
        this.gapsService.getRoleList().subscribe((data: any) => {
            this.roleList =[];
            this.roleListRepositry = data;
            data.forEach(item => {
              this.roleList.push({label: item.name, value: item.name});
            });
            let roleId = this.roleListRepositry.filter(item => item.value === user.roleId);
            this.rolename = roleId[0].name;
           // console.log(this.rolename);
            if(this.rolename=="Physician"){
                this.provider = true;
                this.closeGapForm.controls['payerComments'].disable();
                this.closeGapForm.controls['priority'].disable();
                //this.closeGapForm.controls['closeGap'].disable();
            }
            else{
                this.closeGapForm.controls['providerComments'].disable();
            }
        });
       
       
        this.gapsService.getGapsInfo(this.gapId, this.memberId).subscribe((data: CloseGaps) => {
            this.closeGaps = data;
         //  console.log(data)
            const gapsArray = this.closeGaps.careGaps;
            if (gapsArray.length) {
                this.gaps = gapsArray;
                //console.log(gapsArray);
               this.closeGapForm.controls['priority'].setValue(gapsArray[0].priority);
                this.closeGapForm.controls['status'].setValue(gapsArray[0].status);
                this.closeGapForm.controls['intervention'].setValue(gapsArray[0].intervention);
                this.closeGapForm.controls['payerComments'].setValue(gapsArray[0].payerComments);
                this.closeGapForm.controls['providerComments'].setValue(gapsArray[0].providerComments);
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
       this.firstrow = this.gaps[0];
       this.remainingrows =[];
       for(let i=0;i<this.gaps.length;i++){
         this.remainingrows.push(this.gaps[i])
       }

        });
           
        this.cols = [
            { field: 'payerComments', header: 'Payer Comments' },
            { field: 'priority', header: 'Priority' },
            { field: 'providerComments', header: 'Provider Comments' },
            // { field: 'gapDate', header: 'Date Time' },
            { field: 'gapDate', header: 'Action Date' },
            { field: 'uploadList',header: 'Attachment'}
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
      myUploader(event){
        this.SelectedFile= <File>event.target.files[0];
        this.fd = new FormData();
      this.fd.append('file',  this.SelectedFile);
    }
    type:any;
    onSubmit(formModel) {
      //  console.log(this.targetDate)
        this.gapsService.updateCloseGaps(formModel,this.targetDate,this.closeGaps,this.gapId,this.headers).subscribe((res: any) => {

            if (res.status === 'SUCCESS') {
                this.closeGapCreated = true;
                this.msgService.success('Closegap updated Successfully');
            } 
            else
            {
            this.msgService.error(res.message);
            }
        });
        if(this.closeGapCreated && this.fd){
        this.type = "close_gap";
        this.MemberGapService.commonfileupload(this.type,this.fd).subscribe((res1:any)=>
        {
            if(res1.status === 'SUCCESS'){
                this.fileUploadStatus = true;
                this.msgService.success('Closegap file upload Successfully');
            }
            else {
                this.msgService.error(res1.message);
            }
        }) 
        }
        // for(let num of this.closeGaps.careGaps){
        //   if(num.status === formModel.status && this.formatDate(num.dateTime) === this.formatDate(this.now)){
        //         this.dateFlag = 1;
        //       // console.log(num.dateTime);
        //     }
        // }
        // console.log(this.dateFlag);
        // if(this.dateFlag !=1){
        // if( this.flag == 1 && formModel.status!= 'Open'){
        //  this.msgService.error("already closed by payer you can only change it to open");
        // }

        //   else{
        //     console.log("came here too ")
        //     this.gapsService.updateCloseGaps(formModel, this.memberId, this.gapId).subscribe((res: any) => {
        //         if (res.status === 'SUCCESS') {
        //             this.msgService.success('Closegap updated Successfully');
        //             // this.router.navigateByUrl('/measureworklist?fetch');
        //           } else {
        //             this.msgService.error(res.message);
        //           }
        //     }); 
        //   }
    
        //   }
        //   else{
        //       this.msgService.success("The record already has a same status at same date")
        //   }
        }
        displayAllData: boolean = false;
        showAllDialog(){
          this.displayAllData = true;
        }
        mitButton: boolean = false;
        actionCare(event){
            if(event.value == "MIT"){
                this.mitButton = true;
            }
            else{
                this.mitButton =false;
            }
        }
        downloadFile(event){
            //console.log(event);
            const filetype = event.uploadList[0].slice(-3);
         //   console.log(filetype)
            if(filetype=="png"||filetype=="jpg"){
                this.MemberGapService.downloadimage(event.uploadList[0]).subscribe((res:any)=> {
                    this.saveimage(res)
               if(res){
                   this.msgService.success('Download Successfully');
               }
               else {
                  this.msgService.error(res.message);
                }
           })
            }
            else{
                this.MemberGapService.downloadfile(event.uploadList[0]).subscribe((res:any)=> {
                    this.savefile(res)
               if(res){
                   this.msgService.success('Download Successfully');
               }
               else {
                  this.msgService.error(res.message);
                }
           })
            }
    }
    savefile(data){
    var blob = new Blob([(<any>data)], {type: 'text/csv'});
    var url= window.URL.createObjectURL(blob);
    var anchor = document.createElement("a");
    anchor.download = "closeGaps.csv";
    anchor.href = url;
    anchor.click();
    console.log();
    }
    saveimage(data){
        var blob = new Blob([(<any>data)], {type: 'mime'});
        var url= window.URL.createObjectURL(blob);
        var anchor = document.createElement("a");
        anchor.download = "closeGaps.jpg";
        anchor.href = url;
        anchor.click();
        console.log();
        }
        mitscreen(){
            this.router.navigate(['/pat-screen', this.memberId,this.gapId]);  
        }
       
} 
export interface MemberGap{

}


