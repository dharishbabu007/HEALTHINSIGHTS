import { NgModule } from '@angular/core';
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';
import { CommonModule } from '@angular/common';
import { TranslateModule } from '@ngx-translate/core';
import { NgbDropdownModule } from '@ng-bootstrap/ng-bootstrap';
import { HealthRoutingModule } from './health-routing.module';
import { HealthComponent } from './health.component';
import { PageHeaderModule } from '../../shared/modules/page-header/page-header.module';
import { SidebarComponent } from './components/sidebar/sidebar.component';
import { SidebarNavComponent, SidebarNavDropdownComponent, SidebarNavItemComponent,
    NavDropdownDirective, NavDropdownToggleDirective,
        SidebarNavLinkComponent, SidebarNavTitleComponent } from './components/sidebar/sidebar-nav.component';
import { SidebarMinimizerComponent } from './components/sidebar/sidebar-minimizer.component';
import { SidebarFooterComponent } from './components/sidebar/sidebar-footer.component';
import { SidebarFormComponent } from './components/sidebar/sidebar-form.component';
import { SidebarHeaderComponent } from './components/sidebar/sidebar-header.component';
import { AsideComponent } from './components/sidebar/aside.component';
import { HeaderComponent } from './components/header/header.component';
import { SidebarToggleDirective, AsideToggleDirective, SidebarMinimizeDirective,
  MobileSidebarToggleDirective, SidebarOffCanvasCloseDirective, BrandMinimizeDirective } from './healLayout.directive';
import { HomeComponent } from './home/home.component';
import { EnrollmentsComponent } from './enrollments/enrollments.component';
import { ModelValidationComponent } from './model-validation/model-validation.component';
import { CreatePersonaComponent } from './create-persona/create-personas.component';
import { ViewPersonaComponent } from './view-persona/view-persona.component';
import { GoalsComponent } from './goals/goals.component';
import { TableModule } from 'primeng/table';
import {TabViewModule} from 'primeng/tabview';
import {DialogModule} from 'primeng/dialog';
import {TooltipModule} from 'primeng/tooltip';
import {PickListModule} from 'primeng/picklist';
import { FormsModule,ReactiveFormsModule  } from '@angular/forms';
import { DropdownModule } from 'primeng/dropdown';
import {ChartModule} from 'primeng/chart';
import { FileUploadModule } from 'primeng/fileupload';
import { CalendarModule } from 'primeng/calendar';
import { AutoCompleteModule } from 'primeng/autocomplete';
import { RewardsComponent } from './rewards/rewards.component';
import {RadioButtonModule} from 'primeng/radiobutton';
import { FrameUrlComponent } from './frame-url/frame-url.component';
import { ClusterStatisticsComponent } from './cluster-statistics/cluster-statistics.component';
import {LikelihoodStatisticsComponent} from './likelihood-Statistics/likelihood-statistics.component';
import { CoordinatorHomeComponent } from './coordinator_home/care_home.component';
import { AnalystHomeComponent } from './analyst_home/analyst_home.component';
import { DirectorHomeComponent } from './director_home/director_home.component';


@NgModule({
  imports: [
    CommonModule,
    HealthRoutingModule,
    PageHeaderModule,
    NgbModule,
    NgbDropdownModule, 
     TranslateModule,
     TableModule,
     TabViewModule,
     DialogModule,
     TooltipModule,
     PickListModule,
     FormsModule,
     DropdownModule,
     ReactiveFormsModule,
     ChartModule,
     FileUploadModule,
     CalendarModule,
     AutoCompleteModule,
     RadioButtonModule
    
  ],
  declarations: [HealthComponent,
    SidebarComponent,
    SidebarMinimizerComponent,SidebarFooterComponent,SidebarFormComponent,SidebarHeaderComponent,AsideComponent,HeaderComponent,
    SidebarNavComponent, SidebarNavDropdownComponent, SidebarNavItemComponent,
    NavDropdownDirective, NavDropdownToggleDirective,
        SidebarNavLinkComponent, SidebarNavTitleComponent,
        SidebarToggleDirective, AsideToggleDirective, SidebarMinimizeDirective,
        MobileSidebarToggleDirective, SidebarOffCanvasCloseDirective, BrandMinimizeDirective, HomeComponent, EnrollmentsComponent,CreatePersonaComponent,
        ModelValidationComponent,
        ViewPersonaComponent,
        GoalsComponent,
        RewardsComponent,
        FrameUrlComponent,
        ClusterStatisticsComponent,
        LikelihoodStatisticsComponent,
        CoordinatorHomeComponent,
        AnalystHomeComponent,
        DirectorHomeComponent
  ]
})
export class HealthModule { }
