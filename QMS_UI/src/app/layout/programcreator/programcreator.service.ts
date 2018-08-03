import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../shared/services/http-error-handler.service';
import { ProgramCreator } from './programcreator.component';

const httpOptions = {
  headers: new HttpHeaders({
    'Access-Control-Allow-Origin': '*',
    'Content-Type':  'application/json'
  })
};


@Injectable({
  providedIn: 'root'
})
export class ProgramcreatorService {


  private handleError: HandleError;

  constructor(private http: HttpClient,
    httpErrorHandler: HttpErrorHandler) {
      this.handleError =  httpErrorHandler.createHandleError('ProgramcreatorService');
     }
  programCreatorSubmit(model: ProgramCreator): Observable<ProgramCreator> {
        console.log('ProgramCreator');

        return this.http.post<ProgramCreator>('http://healthinsight:8082/curis/qms/createProgram', model, httpOptions)
        . pipe(
            catchError(this.handleError('programCreatorSubmit', model))
          );
       // this.http.post('http://<hostname>:<port>/curis/qms/createProgram',model);
  }


}
