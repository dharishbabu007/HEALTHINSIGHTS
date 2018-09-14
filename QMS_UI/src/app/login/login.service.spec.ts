import { TestBed, inject } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpErrorHandler } from '../shared/services/http-error-handler.service';
import { LoginService } from './login.service';
import { MessageService } from '../shared/services/message.service';

describe('LoginService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule ],
      providers: [LoginService, HttpErrorHandler, MessageService]
    });
  });

  it('should be created', inject([LoginService], (service: LoginService) => {
    expect(service).toBeTruthy();
  }));
});
