import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InterventionComponent } from './interventions.component';

describe('InterventionComponent', () => {
  let component: InterventionComponent;
  let fixture: ComponentFixture<InterventionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InterventionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InterventionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
