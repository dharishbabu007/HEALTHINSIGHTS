import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { HealthComponent } from './health.component';
import { HomeComponent } from './home/home.component'
import { EnrollmentsComponent } from './enrollments/enrollments.component';
import { ModelValidationComponent } from './model-validation/model-validation.component';
import { CreatePersonaComponent } from './create-persona/create-personas.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { ViewPersonaComponent } from './view-persona/view-persona.component';
import { GoalsComponent } from './goals/goals.component';
import { RewardsComponent } from './rewards/rewards.component';
import { FrameUrlComponent } from './frame-url/frame-url.component';

const routes: Routes = [
  {path: '',component: HealthComponent,
   children: [
      {path: '',redirectTo: 'home', pathMatch: 'prefix' },
      { path: 'home', component: HomeComponent},
      { path: 'enrollments', component: EnrollmentsComponent},
      { path: 'model-validation', component: ModelValidationComponent},
      { path: 'create-persona1', component: CreatePersonaComponent},
      { path: 'view-persona', component: ViewPersonaComponent},
      { path: 'goals', component: GoalsComponent},   
      { path: 'rewards', component: RewardsComponent},  
      { path: 'frame-url/:url', component: FrameUrlComponent },

     ]
}

];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class HealthRoutingModule { }
 