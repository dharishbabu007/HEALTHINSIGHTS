import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { map } from 'rxjs/operators';

@Injectable()
export class AuthenticationService {
    constructor(private http: HttpClient) { }
    
    login(username: string, password: string) {
        return this.http.get<any>(`http://healthinsight:8082/curis/user/login/${username}/${password}`)
            .pipe(
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
    logout() {
        // remove user from local storage to log user out
        localStorage.removeItem('currentUser');
    }
}