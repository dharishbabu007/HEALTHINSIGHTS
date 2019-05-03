import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AnalystHomeComponent } from './analyst_home.component';

describe('AnalystHomeComponent', () => {
  let component: AnalystHomeComponent;
  let fixture: ComponentFixture<AnalystHomeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AnalystHomeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AnalystHomeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
