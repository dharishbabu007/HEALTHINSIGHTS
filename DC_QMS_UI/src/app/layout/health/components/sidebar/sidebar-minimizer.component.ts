import { Component, ElementRef, OnInit  } from '@angular/core';
import { Replace } from '../../../../shared';

@Component({
  selector: 'app-sidebar-minimizer',
  template: `
  `
})
export class SidebarMinimizerComponent implements OnInit {

  constructor(private el: ElementRef) { }

  ngOnInit() {
    Replace(this.el);
  }
}
