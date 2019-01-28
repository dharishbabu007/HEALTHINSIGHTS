import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { CalendarModule } from 'primeng/calendar';
import { PatComponent } from './pat-screen.component';
import { HttpErrorHandler } from '../../shared/services/http-error-handler.service';
import { MessageService } from '../../shared/services/message.service';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
describe('PatComponent', () => {
  let component: PatComponent;
  let fixture: ComponentFixture<PatComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule, ReactiveFormsModule, RouterTestingModule, CalendarModule, BrowserAnimationsModule  ],
      declarations: [ PatComponent ],
      providers: [HttpErrorHandler, MessageService]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PatComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
