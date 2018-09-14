import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../shared/services/http-error-handler.service';
import { Forgot } from './forgot-password.component';
const httpOptions = {
    headers: new HttpHeaders({
      'Access-Control-Allow-Origin': '*',
      'Content-Type':  'application/json'
    })
  };
  
  
  @Injectable({
    providedIn: 'root'
  })
  export class ForgotService {
  
  
    private handleError: HandleError;
  
    constructor(private http: HttpClient,
      httpErrorHandler: HttpErrorHandler) {
        this.handleError =  httpErrorHandler.createHandleError('ForgotService');
       }
    
    Forgot(model: Forgot): Observable<Forgot>{
      console.log(model)
      return this.http.post<Forgot>('http://healthinsight:8082/curis/user/forgot_password',model);
      console.log("forgot password");
  
    }
  
  }
  