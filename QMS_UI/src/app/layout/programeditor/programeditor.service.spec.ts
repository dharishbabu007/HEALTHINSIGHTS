import { TestBed, inject } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpErrorHandler } from '../../shared/services/http-error-handler.service';
import { ProgrameditorService } from './programeditor.service';
import { MessageService } from '../../shared/services/message.service';

describe('ProgrameditorService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule ],
      providers: [ProgrameditorService, HttpErrorHandler, MessageService]
    });
  });

  it('should be created', inject([ProgrameditorService], (service: ProgrameditorService) => {
    expect(service).toBeTruthy();
  }));
});
