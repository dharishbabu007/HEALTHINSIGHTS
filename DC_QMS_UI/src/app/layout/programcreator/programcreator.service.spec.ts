import { TestBed, inject } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpErrorHandler } from '../../shared/services/http-error-handler.service';
import { ProgramcreatorService } from './programcreator.service';
import { MessageService } from '../../shared/services/message.service';

describe('ProgramcreatorService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule ],
      providers: [ProgramcreatorService, HttpErrorHandler, MessageService]
    });
  });

  it('should be created', inject([ProgramcreatorService], (service: ProgramcreatorService) => {
    expect(service).toBeTruthy();
  }));
});
