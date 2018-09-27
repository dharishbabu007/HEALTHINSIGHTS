import { Component, OnInit } from '@angular/core';
import { navItems } from './_nav';
import { Router } from '@angular/router';
import {AnnyangService} from '../shared/services/annyang.service';
@Component({
    selector: 'app-layout',
    templateUrl: './layout.component.html',
    styleUrls: ['./layout.component.scss']
})
export class LayoutComponent {
  public navItems = navItems;
  public micEnabled = true;
  public sidebarMinimized = true;
  private changes: MutationObserver;
  public element: HTMLElement = document.body;

  username: any;
  constructor(private router: Router,private annyang: AnnyangService) {

    this.changes = new MutationObserver((mutations) => {
      this.sidebarMinimized = document.body.classList.contains('sidebar-minimized');
    });

    this.changes.observe(<Element>this.element, {
      attributes: true
    });
  }
  toggleMic() {
    this.micEnabled = !this.micEnabled;
  }


  ngOnInit() {
    var user =  JSON.parse(localStorage.getItem('currentUser'));
         
       this.username= user.loginId;



        if(this.annyang.start){
          console.log("started")
        }
        this.annyang.start();
        this.annyang.commands = {
          'Open Quality Central': (val)=>{
        
              console.log("command start")
        
              this.newFun(val)
          },
          'Open Quality Measure': (val)=>{
        
            console.log("command start")
      
            this.newFun(val)
        },
        'Open My Measures': (val)=>{
        
          console.log("command start")
    
          this.newFun(val)
      },
      'Open Program Creator': (val)=>{
        
        console.log("command start")
  
        this.newFun(val)
    }
    }
  }
  newFun(val){
    this.router.navigate['./quality-central']
 
   }
    
}
