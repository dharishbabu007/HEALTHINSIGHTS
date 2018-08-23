import { TestBed, inject } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpErrorHandler } from '../../shared/services/http-error-handler.service';
import { UserSettingService } from './userSetting.service';
import { MessageService } from '../../shared/services/message.service';

describe('UserSettingService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule ],
      providers: [UserSettingService, HttpErrorHandler, MessageService]
    });
  });

  it('should be created', inject([UserSettingService], (service: UserSettingService) => {
    expect(service).toBeTruthy();
  }));
});
