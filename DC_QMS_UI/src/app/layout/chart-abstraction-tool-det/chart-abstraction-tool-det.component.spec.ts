import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChartAbstractionToolDetComponent } from './chart-abstraction-tool-det.component';

describe('ChartAbstractionToolDetComponent', () => {
  let component: ChartAbstractionToolDetComponent;
  let fixture: ComponentFixture<ChartAbstractionToolDetComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChartAbstractionToolDetComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChartAbstractionToolDetComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
