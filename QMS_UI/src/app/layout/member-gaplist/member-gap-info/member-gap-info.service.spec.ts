import { TestBed, inject } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpErrorHandler } from '../../../shared/services/http-error-handler.service';
import { MemberGapService } from './member-gap-info.service';
import { MessageService } from '../../../shared/services/message.service';

describe('MemberGapService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule ],
      providers: [MemberGapService, HttpErrorHandler, MessageService]
    });
  });

  it('should be created', inject([MemberGapService], (service: MemberGapService) => {
    expect(service).toBeTruthy();
  }));
});
