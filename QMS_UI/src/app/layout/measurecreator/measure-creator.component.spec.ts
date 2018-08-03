import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { MeasurecreatorComponent } from './measure-creator.component';
import { HttpErrorHandler } from '../../shared/services/http-error-handler.service';
import { MessageService } from '../../shared/services/message.service';
describe('MeasurecreatorComponent', () => {
  let component: MeasurecreatorComponent;
  let fixture: ComponentFixture<MeasurecreatorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule, ReactiveFormsModule, RouterTestingModule ],
      declarations: [ MeasurecreatorComponent ],
      providers: [HttpErrorHandler, MessageService]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MeasurecreatorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
