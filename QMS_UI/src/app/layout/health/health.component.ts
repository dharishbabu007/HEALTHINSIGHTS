

import { Component, OnInit } from '@angular/core';
import { navItems } from './_nav';
import { navItems1 } from './_nav';
import { navItems2 } from './_nav';
import {demoRole} from './_nav'
import { Router } from '@angular/router';
import { HttpClient } from '@angular/common/http';
import {GapsService }from '../../shared/services/gaps.service';


@Component({
    selector: 'app-health',
    templateUrl: './health.component.html',
    styleUrls: ['./health.component.scss'],
})
export class HealthComponent implements OnInit {

 
  public navItems = navItems;
  public navItems1 = navItems1;
  public navItems2 = navItems2;
  public demoRole = demoRole;
  public micEnabled = true;
  private changes: MutationObserver;
  public element: HTMLElement = document.body;
  coordinator:boolean = false;
  analyst: boolean = false;
  username: any;
  roleList: any;
  rolename: any;
  qulaity_director: any;
  demo_user: any;
  constructor(private router: Router,
    private http: HttpClient,
    private GapsService:GapsService) {

  }
  ngOnInit() {
    this.coordinator = false;
    this.analyst = false;
    var user = JSON.parse(localStorage.getItem('currentUser'));
    this.username = user.loginId;
    this.GapsService.getRoleList().subscribe((data: any) => {
      this.roleList =[];
      let rolename1 = data.filter(item => item.value === user.roleId);
      this.rolename = rolename1[0].name;
     // console.log(this.rolename)
      if(this.rolename =="Care Coordinator"){
        this.coordinator = true;
      }
      else if(this.rolename =="Business Analyst"){
        this.analyst = true;
      }
      else if(this.rolename == "Quality_Director"){
        this.qulaity_director = true;
      }
      else if(this.rolename == "Demo"){
        this.demo_user = true;
      }
      else{
        this.coordinator = true;
      }
   });  
  
  }
  toggleMic() {
    this.micEnabled = !this.micEnabled;
  }

}
