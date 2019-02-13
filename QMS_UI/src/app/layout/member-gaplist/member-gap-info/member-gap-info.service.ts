import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../../shared/services/http-error-handler.service';
import { MemberGap } from './member-gap-info.component';


const httpOptions = {
  headers: new HttpHeaders({
    'Access-Control-Allow-Origin': '*',
    'Content-Type':  'application/json'
  })
};

 
@Injectable({
  providedIn: 'root'
})
export class MemberGapService {

    headers={
        headers: new HttpHeaders({
          'Content-Type':  'application/json',
          'Authorization': 'my-auth-token'
        })
       }

  private handleError: HandleError;

  constructor(private http: HttpClient,
    httpErrorHandler: HttpErrorHandler,
   ) {
      this.handleError =  httpErrorHandler.createHandleError('MemberGapService');
     }

 
     uploadCloseGapFiles(model: MemberGap): Observable<MemberGap> {

        return this.http.post<MemberGap>('http://healthinsight:8082/curis/closeGaps/gic_lifecycle_import/',model)
        . pipe(
            catchError(this.handleError('Upload', model))
          );

       // this.http.post('http://<hostname>:<port>/curis/qms/createProgram',model);
  }

}