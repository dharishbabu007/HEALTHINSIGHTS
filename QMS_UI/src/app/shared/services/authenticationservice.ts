import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { Observable, of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { MessageService } from './message.service';
import { environment } from '../../environments/environment';

@Injectable()
export class AuthenticationService {
    constructor(private http: HttpClient, private router: Router, private messageSevice: MessageService) { }

    private getStoredUser(): any {
        const rawUser = sessionStorage.getItem('currentUser') ?? localStorage.getItem('currentUser');
        if (!rawUser) {
            return null;
        }

        try {
            return JSON.parse(rawUser);
        } catch (error) {
            sessionStorage.removeItem('currentUser');
            localStorage.removeItem('currentUser');
            return null;
        }
    }

    private handleAuthError(err: HttpErrorResponse): Observable<any> {
        if (err.status === 401) {
            sessionStorage.removeItem('currentUser');
            localStorage.removeItem('currentUser');
            this.router.navigate(['/login']);
            return of(err.message);
        }

        this.messageSevice.error('caught unexpected error');
        throw Error(err.message || 'Unexpected authentication error');
    }

    login(username: string, password: string) {
        return this.http.get<any>(`${environment.apiUrl}/curis/user/login/${encodeURIComponent(username)}/${encodeURIComponent(password)}`)
            .pipe(
                catchError((error) => {
                    this.messageSevice.error(error?.error?.errorMessage || 'Login failed');
                    return of(error);
                }),
                map(user => {
                    if (user) {
                        sessionStorage.setItem('currentUser', JSON.stringify(user));
                        localStorage.removeItem('currentUser');
                    }
                    return user;
                })
            );
    }

    Reset(loginId, oldPassword, newPassword, confirmPassword) {
        return this.http.post<any>(`${environment.apiUrl}/curis/user/reset_password/`, {
            userId: loginId,
            oldPassword: oldPassword,
            newPassword: newPassword,
            conformPassword: confirmPassword
        });
    }

    Register(model) {
        return this.http.post<any>(`${environment.apiUrl}/curis/user/create_user/`, {
            name: model.name,
            email: model.email,
            loginId: model.name,
            firstName: model.firstName,
            lastName: model.lastName,
            securityQuestion: model.securityQuestion,
            securityAnswer: model.securityAnswer,
            phoneNumber: model.phNumber,
            password: model.password
        }).pipe(
            catchError((error) => {
                this.messageSevice.error(error?.error?.message || 'Registration failed');
                return of(error);
            })
        );
    }

    logout() {
        sessionStorage.removeItem('currentUser');
        localStorage.removeItem('currentUser');
    }

    public isAuthenticated(): boolean {
        return !!this.getStoredUser();
    }

    error(message: string, keepAfterNavigationChange = false) {
        return null;
    }
}
