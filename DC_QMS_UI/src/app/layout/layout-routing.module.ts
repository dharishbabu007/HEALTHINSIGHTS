import { NgModule } from '@angular/core';
import { Routes, RouterModule, CanActivate } from '@angular/router';
import { LayoutComponent } from './layout.component';
import { MemberCareGapListComponent } from './member-care-gaplist/member-care-gaplist.component';
import { MemberGapListComponent } from './member-gaplist/member-gaplist.component';
import { MemberGapInfoComponent } from './member-gaplist/member-gap-info/member-gap-info.component';
import { ProgramcreatorComponent } from './programcreator/programcreator.component';
import { ProgrameditorComponent } from './programeditor/programeditor.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { FrameUrlComponent } from './frame-url/frame-url.component';
import { QualityCentralComponent } from './quality-central/quality-central.component';
import { MeasureworklistComponent } from './measureworkilst/measure-worklist.component';
import { MeasurelibraryComponent } from './measurelibrary/measure-library.component';
import { MemberListComponent } from './member-list/member-list.component';
import { MeasurecreatorComponent } from './measurecreator/measure-creator.component';
import { SpvNewComponent } from './spv-new/spv-new.component';
import { ConfiguratorComponent } from './configurator/configurator.component';
import { UserSettingComponent } from './usersettings/userSetting.component';
import { MemberEngageComponent } from './member-engage/member-engage.component';
import { FileManagerComponent } from './file-manager/file-manager.component';
import { Csv1Component } from './csv1/csv1.component';
import { Csv2Component } from './csv2/csv2.component';
import { CreateRoleComponent } from './create-role/create-role.component';
import { CreateUserComponent } from './create-user/create-user.component';
import { NoShowGapListComponent } from './noshowG/noshowG.component';
import {  RoleGuardService} from '../shared/services/RouteGuard.service';
import { ClusterAnalysisComponent } from './cluster-analysis/cluster-analysis.component';
import { CreatePersonasComponent } from './create-personas/create-personas.component';
import { LikelihoodToChurnComponent } from './like-hood-churn/like-hood-churn.component';
import { ClusterStatisticsComponent } from './cluster-statistics/cluster-statistics.component';
import { ViewPersonaComponent } from './view-personas/view-personas.component';
import { LikelihoodStatisticsComponent } from './likelihood-Statistics/likelihood-statistics.component';
import { PersonaDataComponent } from './persona-data/persona-data.component';
import { ClusterMemberListComponent } from './cluster-memberlist/cluster-memberlist.component';
import { SpvComponent } from './spv/spv.component';
import { NonComplianceComponent } from './non-compliance/non-compliance.component';
import { HerniaRepairComponent } from './hernia-repair/hernia-repair.component';
import { CommunicationStatisticsComponent } from  './communication-stats/communication-stats.component';
import { CommunicationToEnrollComponent } from './communication-enroll/communication-enroll.component';
import { PatComponent } from './pat-screen/pat-screen.component';
import { RStudioComponent } from './r-studio/frame-url.component';
import {  SmvComponent } from './smv/smv.component';
import { NcStatsComponent } from './non-compliance-statistics/ncStats.component';
import { PersonaClusteringComponent } from './persona-clustering/persona-clustering.component';
import { PCStatsComponent } from './persona-clustering-statistics/pc-stats.component';
import { ChartAbstractionToolComponent } from './chart-abstraction-tool/chart-abstraction-tool.component';
import {ChartAbstractionToolDetComponent} from './chart-abstraction-tool-det/chart-abstraction-tool-det.component'

const routes: Routes = [
    {
        path: '',
        component: LayoutComponent,
        children: [
            { path: '', redirectTo: 'dashboard', pathMatch: 'prefix' },
            { path: 'configurator', component: ConfiguratorComponent },
            { path: 'dashboard', component: DashboardComponent },
            { path: 'frame-url/:url', component: FrameUrlComponent },
            { path: 'member-gap-list', component: MemberGapListComponent ,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '11'
            } },
            { path: 'Member Gap List', component: MemberGapListComponent ,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '11'
            } },
            { path: 'member-gap-list/:memberId', component: MemberGapListComponent ,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '11'
            } },
            { path: 'member-care-gap-list', component: MemberCareGapListComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '9'
            }  },
            { path: 'Member Care Gap Registry', component: MemberCareGapListComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '9'
            }  },
            { path: 'member-gap/:gapId/:memberId', component: MemberGapInfoComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '10'
            }  },
            { path: 'Close Gaps', component: MemberGapInfoComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '10'
            }  },
            { path: 'programcreator', component: ProgramcreatorComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '2'
            } },
            { path: 'Program Creator', component: ProgramcreatorComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '2'
            } },
            { path: 'programeditor', component: ProgrameditorComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '3'
            } },
            { path: 'Program Editor', component: ProgrameditorComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '3'
            } },
            { path: 'Quality Central', component: QualityCentralComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '1'
            }  },
            { path: 'Quality Central', component: QualityCentralComponent,   canActivate: [RoleGuardService],
            data: { 
              expectedRole: '1'
            }  },
            { path: 'measureworklist', component: MeasureworklistComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '7'
            } },
            { path: 'My Measures', component: MeasureworklistComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '7'
            } },
            { path: 'measurelibrary', component: MeasurelibraryComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '6'
            } },
            { path: 'Quality Measures', component: MeasurelibraryComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '6'
            } },
            { path: 'measurelibrary/:type/:value', component: MeasurelibraryComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '6'
            } },
            { path: 'member-list', component: MemberListComponent},
            { path: 'member-list/:type', component: MemberListComponent},
            { path: 'measurecreator', component: MeasurecreatorComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '4'
            } },
            { path: 'Measure Creator', component: MeasurecreatorComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '4'
            } },
            { path: 'Measure Editor', component: MeasurecreatorComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '4'
            } },
            { path: 'measurecreator/:measureId/:type', component: MeasurecreatorComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '4' || '5'
            } },
            { path: 'spv/:memberId', component: SpvComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '8'
            } },
            { path: 'Single Patient View', component: SpvComponent,   canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '8'
            } },
            { path: 'smv/:memberId', component: SmvComponent},
            { path: 'usersettings', component: UserSettingComponent},
            { path: 'file-manager', component: FileManagerComponent},
            { path: 'Use Cases', component: FileManagerComponent},
            { path: 'csv1/:selectedModel', component: Csv1Component },
            { path: 'csv2', component: Csv2Component},
            { path: 'create-role', component: CreateRoleComponent,  canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '13'
            } },
            { path: 'Role Mapping', component: CreateRoleComponent,  canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '13'
            } },
            { path: 'create-user', component: CreateUserComponent,  canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '14'
            } },
            { path: 'User Mapping', component: CreateUserComponent,  canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '14'
            } },
            { path: 'noshowGapList', component: NoShowGapListComponent},
            { path: 'memberEngagement', component: MemberEngageComponent},
            { path: 'create-personas', component: CreatePersonasComponent},
            { path: 'cluster-analysis', component: ClusterAnalysisComponent},
            { path: 'likelihoodChurn', component: LikelihoodToChurnComponent},
            { path: 'likelihood/:type', component: LikelihoodToChurnComponent},
            { path: 'cluster-statistics', component: ClusterStatisticsComponent},
            { path: 'create-persona', component: ViewPersonaComponent},
            { path: 'likelihood-statistics/:type', component: LikelihoodStatisticsComponent},
            { path: 'view-persona', component:PersonaDataComponent},
            { path: 'cluster-memberlist', component:ClusterMemberListComponent},
            { path: 'spv1/:memberId', component: SpvNewComponent},
            { path: 'non-compliance', component: NonComplianceComponent},
            { path: 'non-compliance/:redirect', component: NonComplianceComponent},
            { path: 'hernia-repair', component: HerniaRepairComponent},
            { path: 'communication-stats', component: CommunicationStatisticsComponent},
            { path: 'communication-enroll', component:CommunicationToEnrollComponent},
            { path: 'pat-screen', component:PatComponent},
            { path: 'pat-screen/:memberId/:measureSK/:plan', component:PatComponent},
            { path: 'pat-screen/:memberId/:measureSK', component:PatComponent},
            { path: 'MIT', component:PatComponent},
            { path: 'r-studio', component:RStudioComponent},
            { path: 'ncStats', component:NcStatsComponent},
            { path: 'persona-clustering', component:PersonaClusteringComponent},
            { path: 'pc-stats', component:PCStatsComponent},
            { path: 'chart-abstract', component:ChartAbstractionToolComponent,  canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '12'
            } },
            { path: 'Chart Abstraction Tool', component:ChartAbstractionToolComponent,  canActivate: [RoleGuardService], 
            data: { 
              expectedRole: '12'
            } },
            { path: 'chart-abstract-det', component:ChartAbstractionToolDetComponent}

        ]
    },
    { path: 'health', loadChildren: './health/health.module#HealthModule' },
    { path: 'Healthy Me', loadChildren: './health/health.module#HealthModule' },
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class LayoutRoutingModule {}
