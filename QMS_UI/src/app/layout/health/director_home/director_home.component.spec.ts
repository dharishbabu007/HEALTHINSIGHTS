import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DirectorHomeComponent } from './director_home.component';

describe('DirectorHomeComponent', () => {
  let component: DirectorHomeComponent;
  let fixture: ComponentFixture<DirectorHomeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DirectorHomeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DirectorHomeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
