import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../shared/services/http-error-handler.service';
import { Login } from './login.component';
import { ResetPassword } from '../reset-password/reset-password.component';
import { map } from 'rxjs/operators';


const httpOptions = {
  headers: new HttpHeaders({
    'Access-Control-Allow-Origin': '*',
    'Content-Type':  'application/json'
  })
};


@Injectable({
  providedIn: 'root'
})
export class LoginService {


  private handleError: HandleError;

  constructor(private http: HttpClient,
    httpErrorHandler: HttpErrorHandler) {
      this.handleError =  httpErrorHandler.createHandleError('LoginService');
     }
  LoginSubmit(username: string, password: string): Observable<Login> {
        console.log("Login");
        console.log(username)
        return this.http.get<Login>(`http://healthinsight:8082/curis/user/login/${username}/${password}`).pipe(map(user => {
          // login successful if there's a jwt token in the response
          if (user) {
              // store user details and jwt token in local storage to keep user logged in between page refreshes
              localStorage.setItem('currentUser', JSON.stringify(user));
          }
          return user;
      }));

        // . pipe(
        //     // catchError(this.handleError('LoginSubmit', model))
        //   );
       // this.http.post('http://<hostname>:<port>/curis/qms/createProgram',model);
  }
  Reset(model: ResetPassword): Observable<ResetPassword>{
    return this.http.post<ResetPassword>('http://healthinsight:8082/curis/user/reset_password/',model);
  }

}
