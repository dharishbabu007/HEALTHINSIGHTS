import { TestBed, inject } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpErrorHandler } from '../../shared/services/http-error-handler.service';
import { PatScreenService } from './pat-screen-service';
import { MessageService } from '../../shared/services/message.service';

describe('PatScreenService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule ],
      providers: [PatScreenService, HttpErrorHandler, MessageService]
    });
  });

  it('should be created', inject([PatScreenService], (service: PatScreenService) => {
    expect(service).toBeTruthy();
  }));
});
