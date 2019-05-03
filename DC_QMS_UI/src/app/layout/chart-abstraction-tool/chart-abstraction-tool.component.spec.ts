import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChartAbstractionToolComponent } from './chart-abstraction-tool.component';

describe('ChartAbstractionToolComponent', () => {
  let component: ChartAbstractionToolComponent;
  let fixture: ComponentFixture<ChartAbstractionToolComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChartAbstractionToolComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChartAbstractionToolComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
