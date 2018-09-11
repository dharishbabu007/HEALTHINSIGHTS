import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { FileManagerComponent } from './file-manager.component';
import { HttpErrorHandler } from '../../shared/services/http-error-handler.service';
import { MessageService } from '../../shared/services/message.service';
describe('FileManagerComponent', () => {
  let component: FileManagerComponent;
  let fixture: ComponentFixture<FileManagerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule, ReactiveFormsModule, RouterTestingModule ],
      declarations: [ FileManagerComponent ],
      providers: [HttpErrorHandler, MessageService]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FileManagerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
