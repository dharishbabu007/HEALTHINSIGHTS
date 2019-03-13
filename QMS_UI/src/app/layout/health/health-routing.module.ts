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
import { ClusterStatisticsComponent } from './cluster-statistics/cluster-statistics.component';
import {LikelihoodStatisticsComponent} from './likelihood-Statistics/likelihood-statistics.component';
import { CoordinatorHomeComponent } from './coordinator_home/care_home.component';
import { AnalystHomeComponent } from './analyst_home/analyst_home.component';
import { DirectorHomeComponent } from './director_home/director_home.component';
import { PersonaMemberListComponent } from './persona-member-list/member-list.component';
import { GoalsMemberListComponent } from './goals-member-list/goals-member-list.component';
import { RewardsMemberListComponent } from './rewards-member-list/rewards-member-list.component';
import {  SmvComponent } from './smv/smv.component';
import { InterventionComponent } from './interventions/interventions.component';
import { ChurnMemberListComponent } from './likelihood-churn-list/churn-member-list.component';
import { RecommendMemberListComponent } from './likelihood-to-recommend-list/recommend-member-list.component';
const routes: Routes = [
  {path: '',component: HealthComponent,
   children: [
      {path: '',redirectTo: 'home', pathMatch: 'prefix' },
      { path: 'home', component: HomeComponent},
      { path: 'enrollments', component: EnrollmentsComponent},
      { path: 'model-validation', component: ModelValidationComponent},
      { path: 'create-persona1', component: CreatePersonaComponent},
      { path: 'view-persona/:clusterId', component: ViewPersonaComponent},
      { path: 'goals', component: GoalsComponent},   
      { path: 'goals/:memberId', component: GoalsComponent},   
      { path: 'rewards', component: RewardsComponent},  
      { path: 'frame-url/:url', component: FrameUrlComponent },
      { path: 'cluster-statistics',component: ClusterStatisticsComponent},
      { path: 'likelihood/:type',component: LikelihoodStatisticsComponent},
      {path:'care_home',component:CoordinatorHomeComponent},
      { path:'analyst_home',component:AnalystHomeComponent},
      { path:'director_home',component:DirectorHomeComponent},
      { path:'persona-member-list',component:PersonaMemberListComponent},
      { path:'goals-member-list',component:GoalsMemberListComponent},
      { path:'rewards-member-list',component: RewardsMemberListComponent},
      { path:'smv',component: SmvComponent},
      { path:'intervention',component: InterventionComponent},
      { path:'smv/:memberId',component: SmvComponent},
      { path:'churn-member-list',component: ChurnMemberListComponent},
      { path:'recommend-member-list',component: RecommendMemberListComponent},

     ]
}

];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class HealthRoutingModule { }
 