import { Component, OnInit } from '@angular/core';
import { navItems } from './_nav';
import { Router } from '@angular/router';
import {AnnyangService} from '../shared/services/annyang.service';
import {Idle, DEFAULT_INTERRUPTSOURCES} from '@ng-idle/core';
import {Keepalive} from '@ng-idle/keepalive';

import { HttpClient } from '@angular/common/http';
import { NgxPermissionsService, NgxRolesService} from 'ngx-permissions';
import { Message } from 'primeng/primeng';
import {ConfirmDialogModule} from 'primeng/confirmdialog';
import {ConfirmationService} from 'primeng/api';
import { GapsService } from '../shared/services/gaps.service';
@Component({
    selector: 'app-layout',
    templateUrl: './layout.component.html',
    styleUrls: ['./layout.component.scss'],
    providers: [GapsService]
})
export class LayoutComponent {
  idleState = 'Not started.';
  timedOut = false;
  lastPing?: Date = null;
  msgs: Message[] = [];
  nome :any = localStorage['app-app-layout'];
  commands: any;
  rolename: any;
  roledata: any;
  roleList: any;
  repositry:any;
  public navItems = navItems;
  public micEnabled = true;
  public sidebarMinimized = true;
  private changes: MutationObserver;
  public element: HTMLElement = document.body;

  username: any;
  constructor(private router: Router,
    private annyang: AnnyangService,
    private idle: Idle,
    private keepalive: Keepalive, 
    private confirmationService: ConfirmationService,
    private permissionsService: NgxPermissionsService,
    private rolesService: NgxRolesService,
    private http: HttpClient,
    private GapsService: GapsService,) {
   
    this.changes = new MutationObserver((mutations) => {
      this.sidebarMinimized = document.body.classList.contains('sidebar-minimized');
    });

    this.changes.observe(<Element>this.element, {
      attributes: true
    });
   if(localStorage.currentUser !== null)
   {
      idle.setIdle(19000);
      // idle.setTimeout(15);
        idle.setInterrupts(DEFAULT_INTERRUPTSOURCES);

        idle.onTimeoutWarning.subscribe((countdown: number) => {
          this.confirm();
            //alert('TimeOut in ' + countdown)
        });
      // idle.onTimeout.subscribe(() => {
        // alert('Timeout');
      

        //  this.router.navigateByUrl('/login');

      // });

        idle.watch();
   }


  }

  ngOnInit() {
    var user =  JSON.parse(localStorage.getItem('currentUser'));
    this.username= user.loginId;
 
    this.GapsService.getRoleData(user.roleId).subscribe((data: any)=>{
      this.roledata = data;
      var perm =[];
      if( this.roledata.screenPermissions[2].read == "Y"){
       
        perm.push("read");
      }
      if (this.roledata.screenPermissions[2].write == "Y"){
       
        perm.push("write");
      }
      if (this.roledata.screenPermissions[2].download == "Y"){
       perm.push("download");
      }
    
      this.permissionsService.loadPermissions(perm);
         
       //this.http.get('url').subscribe((permissions) => {
  
        // this.permissionsService.loadPermissions(perm);
      //  })
       
      let perms = this.permissionsService.getPermissions();
      this.rolesService.addRole(user.role , [user.permissions]);
    
      });

    
     this.commands = {
      'open *': (val)=>{
      console.log("command start")
      this.newFun(val)
      },
      'open quality Central': (val) =>{
      console.log("command start")
      this.newFun1(val)
      }
      }
      
      if(this.annyang){
      this.annyang.start();
      this.annyang.debug();
      console.log("started");
      this.annyang.addcommands(this.commands)
      } 
  
  }
  toggleMic() {
    this.micEnabled = !this.micEnabled;
  }

  newFun(val){
      this.router.navigateByUrl('/' + val);  
  }
  newFun1(val){
    this.router.navigateByUrl('/Quality Central');  
  }
  confirm() {
    this.confirmationService.confirm({
        message: 'Page idle for longtime! sure you want to logout',
        accept: () => {
          localStorage.clear();
          this.router.navigateByUrl('/login');
        
      },
      reject: () => {
          this.msgs = [{severity:'info', summary:'Rejected', detail:'You have rejected'}];
      }
    });
  }
}
