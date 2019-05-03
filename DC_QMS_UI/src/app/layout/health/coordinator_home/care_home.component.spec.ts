import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CoordinatorHomeComponent } from './care_home.component';

describe('CoordinatorHomeComponent', () => {
  let component: CoordinatorHomeComponent;
  let fixture: ComponentFixture<CoordinatorHomeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CoordinatorHomeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CoordinatorHomeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
