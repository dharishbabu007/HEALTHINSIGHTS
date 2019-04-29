import { TestBed, inject } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpErrorHandler } from '../../../shared/services/http-error-handler.service';
import { PersonaService } from './create-persona.service';
import { MessageService } from '../../../shared/services/message.service';

describe('PersonaService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  HttpClientTestingModule ],
      providers: [PersonaService, HttpErrorHandler, MessageService]
    });
  });

  it('should be created', inject([PersonaService], (service: PersonaService) => {
    expect(service).toBeTruthy();
  }));
});
