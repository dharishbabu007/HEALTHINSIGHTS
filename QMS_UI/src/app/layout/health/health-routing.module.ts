import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { HealthComponent } from './health.component';
import { HomeComponent } from './home/home.component'
import { EnrollmentsComponent } from './enrollments/enrollments.component';

const routes: Routes = [
  {path: '',component: HealthComponent,
   children: [
      {path: '',redirectTo: 'home', pathMatch: 'prefix' },
      { path: 'home', component: HomeComponent},
      { path: 'enrollments', component: EnrollmentsComponent}

     ]
}

];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class HealthRoutingModule { }
 