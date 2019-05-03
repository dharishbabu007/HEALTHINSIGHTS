import { Component, ElementRef, OnInit  } from '@angular/core';
import { Replace } from '../../../shared';

@Component({
  selector: 'app-sidebar-form',
  template: `
    <form class="sidebar-form">
      <ng-content></ng-content>
    </form>
  `
})
export class SidebarFormComponent implements OnInit {

  constructor(private el: ElementRef) { }

  ngOnInit() {
    Replace(this.el);
  }
}
