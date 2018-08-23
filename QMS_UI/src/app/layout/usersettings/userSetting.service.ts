import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../shared/services/http-error-handler.service';
import { userSetting } from './UserSetting.component';

const httpOptions = {
  headers: new HttpHeaders({
    'Access-Control-Allow-Origin': '*',
    'Content-Type':  'application/json'
  })
};


@Injectable({
  providedIn: 'root'
})
export class UserSettingService {


  private handleError: HandleError;

  constructor(private http: HttpClient,
    httpErrorHandler: HttpErrorHandler) {
      this.handleError =  httpErrorHandler.createHandleError('UserSettingService');
     }
  UserSettingSubmit(model: userSetting): Observable<userSetting> {
        console.log('UserSetting');

        return this.http.post<userSetting>('http://healthinsight:8082/curis/qms/createProgram', model, httpOptions)
        . pipe(
            catchError(this.handleError('UserSettingSubmit', model))
          );
       // this.http.post('http://<hostname>:<port>/curis/qms/createProgram',model);
  }


}
