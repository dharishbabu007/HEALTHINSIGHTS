import { Injectable } from '@angular/core';
import { Router, CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';

@Injectable()
export class AuthGuard implements CanActivate {
    constructor(private router: Router) {}

    canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot) {
        const rawCurrentUser = sessionStorage.getItem('currentUser') ?? localStorage.getItem('currentUser');

        if (rawCurrentUser) {
            try {
                JSON.parse(rawCurrentUser);
                return true;
            } catch (error) {
                sessionStorage.removeItem('currentUser');
                localStorage.removeItem('currentUser');
            }
        }

        this.router.navigate(['/login'], { queryParams: { returnUrl: state.url } });
        return false;
    }
}
