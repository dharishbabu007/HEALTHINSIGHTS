import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { CalendarModule } from 'primeng/calendar';
import { ProgramcreatorComponent } from './programcreator.component';
import { HttpErrorHandler } from '../../shared/services/http-error-handler.service';
import { MessageService } from '../../shared/services/message.service';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
describe('ProgramcreatorComponent', () => {
  let component: ProgramcreatorComponent;
  let fixture: ComponentFixture<ProgramcreatorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule, ReactiveFormsModule, RouterTestingModule, CalendarModule, BrowserAnimationsModule  ],
      declarations: [ ProgramcreatorComponent ],
      providers: [HttpErrorHandler, MessageService]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProgramcreatorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
