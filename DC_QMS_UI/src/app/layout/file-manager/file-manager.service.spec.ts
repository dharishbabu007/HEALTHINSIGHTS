import { TestBed, inject } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpErrorHandler } from '../../shared/services/http-error-handler.service';
import { FileManagerService } from './file-manager.service';
import { MessageService } from '../../shared/services/message.service';

describe('FileManagerService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule ],
      providers: [FileManagerService, HttpErrorHandler, MessageService]
    });
  });

  it('should be created', inject([FileManagerService], (service: FileManagerService) => {
    expect(service).toBeTruthy();
  }));
});
