import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders,HttpErrorResponse } from '@angular/common/http';
import { Observable ,of} from 'rxjs';
import { catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../shared/services/http-error-handler.service';
import { UserSetting } from './userSetting.component';
import { MessageService } from '../../shared/services/message.service';


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
    httpErrorHandler: HttpErrorHandler,private MessageService:MessageService
   ) {
      this.handleError =  httpErrorHandler.createHandleError('UserSettingService');
     }

     private handleAuthError(err: HttpErrorResponse): Observable<any> {
      //handle your auth error or rethrow
      if (err.status === 401) {
        //navigate /delete cookies or whatever
        console.log('handled error ' + err.status);
        // if you've caught / handled the error, you don't want to rethrow it unless you also want downstream consumers to have to handle it as well.
        return of(err.message);
      }
      throw err;
    }
  UserSettingSubmit(model: UserSetting,loginID): Observable<UserSetting> {
        console.log(loginID);

        return this.http.post<UserSetting>('http://healthinsight:8082/curis/user/update_user/',{
          "email": model.EmailId,
          "loginId": loginID,
          "firstName": model.firstName,
          "lastName": model.lastName,
          "securityQuestion": model.securityQuestion,
          "securityAnswer": model.securityAnswer,
          "phoneNumber": model.phoneNumber
      }
      , httpOptions)
        . pipe(catchError((error, caught) => {
          //intercept the respons error and displace it to the console
          this.MessageService.error(error.error.message);
          this.handleAuthError(error.error.message);
          return of(error);
        }) as any);
       // this.http.post('http://<hostname>:<port>/curis/qms/createProgram',model);
  }

}
