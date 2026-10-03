import { Injectable } from '@angular/core';
import { HttpRequest, HttpHandler, HttpEvent, HttpInterceptor } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable()
export class JwtInterceptor implements HttpInterceptor {
    intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
        const rawCurrentUser = sessionStorage.getItem('currentUser') ?? localStorage.getItem('currentUser');

        if (!rawCurrentUser) {
            return next.handle(request);
        }

        try {
            const currentUser = JSON.parse(rawCurrentUser);
            if (currentUser && currentUser.token) {
                request = request.clone({
                    setHeaders: {
                        Authorization: `Bearer ${currentUser.token}`
                    }
                });
            }
        } catch (error) {
            sessionStorage.removeItem('currentUser');
            localStorage.removeItem('currentUser');
        }

        return next.handle(request);
    }
}
