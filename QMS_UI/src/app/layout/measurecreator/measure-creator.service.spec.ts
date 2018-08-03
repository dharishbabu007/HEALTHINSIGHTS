import { TestBed, inject } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpErrorHandler } from '../../shared/services/http-error-handler.service';
import { MeasurecreatorService } from './measure-creator.service';
import { MessageService } from '../../shared/services/message.service';

describe('MeasurecreatorService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule ],
      providers: [MeasurecreatorService, HttpErrorHandler, MessageService] 
    });
  });

  it('should be created', inject([MeasurecreatorService], (service: MeasurecreatorService) => {
    expect(service).toBeTruthy();
  }));
});
