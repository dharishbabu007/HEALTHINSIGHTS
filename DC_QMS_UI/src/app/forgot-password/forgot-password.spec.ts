import { TestBed, inject } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpErrorHandler } from '../shared/services/http-error-handler.service';
import { MessageService } from '../shared/services/message.service';
import { ForgotService } from './forgot-password.service';
describe('ForgotService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule ],
      providers: [ForgotService, HttpErrorHandler, MessageService]
    });
  });

  it('should be created', inject([ForgotService], (service: ForgotService) => {
    expect(service).toBeTruthy();
  }));
});
