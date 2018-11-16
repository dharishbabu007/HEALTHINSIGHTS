import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import { map } from 'rxjs/operators';

import {
    HttpRequest,
    HttpHandler,
    HttpEvent,
    HttpInterceptor, HttpErrorResponse
  } from '@angular/common/http';
  import {Observable, of} from 'rxjs';
  import {Router} from "@angular/router";
  import {catchError} from "rxjs/internal/operators";

  import { MessageService } from "./message.service";
import { User } from '../modules/user';

@Injectable()
export class AuthenticationService {
    constructor(private http: HttpClient, private router: Router, private messageSevice: MessageService) { }
    private handleAuthError(err: HttpErrorResponse): Observable<any> {
        //handle your auth error or rethrow
        if (err.status === 401) {
          //navigate /delete cookies or whatever
          console.log('handled error ' + err.status);
          this.router.navigate([`/login`]);
          // if you've caught / handled the error, you don't want to rethrow it unless you also want downstream consumers to have to handle it as well.
          return of(err.message);
        }
        else{
            this.messageSevice.error("caught unexpected error")
        }
        throw Error;
      }
      
    login(username: string, password: string) {
        return this.http.get<any>(`http://healthinsight:8082/curis/user/login/${username}/${password}`)
            .pipe(
                catchError((error, caught) => {
                    //intercept the respons error and displace it to the console
                    this.messageSevice.error(error.error.errorMessage);
                    this.handleAuthError(error);
                    return of(error);
                  }),
                map(user => {
                // login successful if there's a jwt token in the response
                
                if (user) {
                    // store user details and jwt token in local storage to keep user logged in between page refreshes
                    localStorage.setItem('currentUser', JSON.stringify(user));
                }
                return user;
            }));
    }
    Reset(loginId,oldPassword,newPassword,confirmPassword){
        console.log(newPassword);
        return this.http.post<any>('http://healthinsight:8082/curis/user/reset_password/',{
             "userId":loginId,
            "oldPassword":oldPassword, 
             "newPassword":newPassword,
            "conformPassword":confirmPassword
            
}
);
      }



      Register(model){
          return this.http.post<any>('http://healthinsight:8082/curis/user/create_user/',{
            "name": model.name,
            "email": model.email,
            "loginId": model.name,
            "firstName": model.firstName,
            "lastName": model.lastName,
            "securityQuestion": model.securityQuestion,
            "securityAnswer": model.securityAnswer,
            "phoneNumber": model.phNumber,
            "password": model.password
        }
        );
      }
    logout() {
        // remove user from local storage to log user out
        localStorage.removeItem('currentUser');
    }

    public isAuthenticated(): boolean {
        var user =  JSON.parse(localStorage.getItem('currentUser'));
        if(user){
           // console.log("true")
        return true;}
        else{
        return false;}
      }

      error(message: string, keepAfterNavigationChange = false) {
    }
}