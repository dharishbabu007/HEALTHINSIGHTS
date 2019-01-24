

import { Component, OnInit } from '@angular/core';
import { navItems } from './_nav';
import { Router } from '@angular/router';
import { HttpClient } from '@angular/common/http';


@Component({
    selector: 'app-health',
    templateUrl: './health.component.html',
    styleUrls: ['./health.component.scss'],
})
export class HealthComponent implements OnInit {

 
  public navItems = navItems;
  public micEnabled = true;
  private changes: MutationObserver;
  public element: HTMLElement = document.body;
  username: any;
  constructor(private router: Router,
    private http: HttpClient,) {

  }
  ngOnInit() {
    var user =  JSON.parse(localStorage.getItem('currentUser'));
    this.username= user.loginId;
  
  }
  toggleMic() {
    this.micEnabled = !this.micEnabled;
  }

}
