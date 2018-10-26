import { Injectable } from '@angular/core';
import { 
  Router,
  CanActivate,
  ActivatedRouteSnapshot
} from '@angular/router';
import { AuthenticationService } from './authenticationservice';
import { MessageService } from './message.service'
@Injectable()
export class RoleGuardService implements CanActivate {
  constructor(public auth: AuthenticationService, public router: Router, private msgService:MessageService) {}
  canActivate(route: ActivatedRouteSnapshot): boolean {
    // this will be passed from the route config
    // on the data property
    const expectedRole = route.data.expectedRole;
    const token = JSON.parse(localStorage.getItem('currentUser'));
    if (
      !this.auth.isAuthenticated() || 
      token.roleId !== expectedRole
    ) {
      this.router.navigate(['dashboard']);
      this.msgService.error('sorry!! you dont have access to view this page')
      return false;
    }
    return true;
  }
}