import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import {  SmvComponent } from './smv.component';

describe(' SmvComponent', () => {
  let component:  SmvComponent;
  let fixture: ComponentFixture< SmvComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [  SmvComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent( SmvComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
