import { Injectable } from '@angular/core';
import { 
  Router,
  CanActivate,
  ActivatedRouteSnapshot
} from '@angular/router';
import { AuthenticationService } from './authenticationservice';
import { MessageService } from './message.service';
import { GapsService } from './gaps.service';
@Injectable()
export class RoleGuardService implements CanActivate {
  roledata: any;
  screens: any;
  flag: any;
  expectedRole: any;
  constructor(public auth: AuthenticationService, public router: Router, private msgService:MessageService, private GapsService: GapsService) {
  }
 canActivate(route: ActivatedRouteSnapshot): boolean {
    // this will be passed from the route config
    // on the data property
    
    this.expectedRole = route.data.expectedRole;
    const path = route.data.path;
    const token = JSON.parse(localStorage.getItem('currentUser'));
    
    const users = this.GapsService.getRoleData1(token.roleId);
                    
    this.GapsService.getRoleData1(token.roleId)
    .then((data: any)=>{
      this.roledata = [];
      this.roledata = data;
      this.screens =[];
     // console.log(this.roledata)
      for( let i=0; i<this.roledata.screenPermissions.length; i++){
        this.screens.push(this.roledata.screenPermissions[i].screenId)
      }
     // console.log("came here 1");
     
     
    }).then((data: any) => {
      if(!this.auth.isAuthenticated() ||
      !this.newfunc()){
      //  console.log("came here 2")
        this.router.navigate(['dashboard']);
        this.msgService.error('sorry!! you dont have access to view this page');
      }
     
    })
    .catch(error => console.log(error));
    
      return true; 
}

newfunc(){
 this.flag = false;
  for( let i=0; i<this.screens.length; i++){
  //  console.log("came here as well");
  //  console.log(this.expectedRole)
    if(this.screens[i] == this.expectedRole){
      this.flag = true;
      break;
    }
   }
   return this.flag;
}

}