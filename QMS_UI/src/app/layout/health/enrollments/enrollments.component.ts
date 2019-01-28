import { Component, OnInit } from '@angular/core';
import { GapsService } from "../../../shared/services/gaps.service";

@Component({
  selector: 'app-enrollments',
  templateUrl: './enrollments.component.html',
  styleUrls: ['./enrollments.component.scss']
})
export class EnrollmentsComponent implements OnInit {
  confusionmatrix: any;
  selected: any[];
  cols: any;
  selectedcar: any;
  constructor(private gaps:GapsService) { 
    this.gaps.getlikelihoodconfusionmatric().subscribe((data: any[]) => {
      this.confusionmatrix = data;
  });

  this.cols = [
      { field: 'id', header: 'ID' },
      { field: 'zero', header: 'Zero' },
      { field: 'one', header: 'One' },
  ];
  }

  ngOnInit() {
  }

}
