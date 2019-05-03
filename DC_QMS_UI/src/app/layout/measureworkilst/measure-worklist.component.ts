import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';
import { MessageService } from '../../shared/services/message.service';
import { Router, ActivatedRoute } from '@angular/router';
import { NgxPermissionsService, NgxRolesService} from 'ngx-permissions';
@Component({
    selector: 'app-tables',
    templateUrl: './measure_worklist.component.html',
    styleUrls: ['./measure-worklist.component.scss'],
    animations: [routerTransition()], 
    providers: [GapsService]
})
export class MeasureworklistComponent implements OnInit {
    refresh = false;
    comments: any;
    constructor(private gapsService: GapsService,
        private msgService: MessageService,
        public router: Router,
        private route: ActivatedRoute,
        private permissionsService: NgxPermissionsService,
        ) {
            this.route.queryParams.subscribe(params => {
                if (params['fetch']) {
                    this.refresh = true;
                }
            });
         }
    membergaps: MemberCareGaps[];
    cols: any[];
    perms: any;
    rowid: any;
    ngOnInit() {
        this.gapsService.getWorkList().subscribe((data: MemberCareGaps[]) => {
            this.membergaps = data;
        });
        this.cols = [
            { field: 'id', header: 'Measure ID', width: '6%'},
            { field: 'name', header: 'Measure Name', width: '20%' },
            { field: 'programName', header: 'Program Name', width: '20%' },
            { field: 'status', header: 'Status', width: '10%' },
            { field: 'reviewComments', header: 'Review Comments' , width: '20%'},
            { field: 'reviewedBy', header: 'Reviewed By' , width: '20%'},
        ];
        this.permissionsService.permissions$.subscribe((permissions) => {
            this.perms = permissions;
          })
    }
    copytoCreator(id, newType) {
          if(this.perms['7W']){  
            if(this.perms['5R']){
            this.router.navigate(['/measurecreator', id, newType]);  
            }
        }
    }
   statusClickImg(status, id) {
       this.gapsService.setMeasureStatus(id, status,this.comments).subscribe( (res: any) => {
           //console.log(res)
        if (res.status === 'SUCCESS') {
            this.msgService.success('Measure approved Successfully');
            window.location.reload();
           // console.log("came here")
          } else {
            this.msgService.error(res.message);
          }
       });
   }
   dialogBox:boolean = false;
   showDialog(event){
    this.dialogBox = true;
    this.rowid = event;
    console.log(event)
   }
   
}
