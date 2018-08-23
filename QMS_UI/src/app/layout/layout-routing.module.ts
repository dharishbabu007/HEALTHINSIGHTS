import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { LayoutComponent } from './layout.component';
import { MemberCareGapListComponent } from './member-care-gaplist/member-care-gaplist.component';
import { MemberGapListComponent } from './member-gaplist/member-gaplist.component';
import { MemberGapInfoComponent } from './member-gaplist/member-gap-info/member-gap-info.component';
import { ProgramcreatorComponent } from './programcreator/programcreator.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { FrameUrlComponent } from './frame-url/frame-url.component';
import { QualityCentralComponent } from './quality-central/quality-central.component';
import { MeasureworklistComponent } from './measureworkilst/measure-worklist.component';
import { MeasurelibraryComponent } from './measurelibrary/measure-library.component';
import { MemberListComponent } from './member-list/member-list.component';
import { MeasurecreatorComponent } from './measurecreator/measure-creator.component';
import { SpvComponent } from './spv/spv.component';
import { ConfiguratorComponent } from './configurator/configurator.component';
import { UserSettingComponent } from './usersettings/userSetting.component';
const routes: Routes = [
    {
        path: '',
        component: LayoutComponent,
        children: [
            { path: '', redirectTo: 'dashboard', pathMatch: 'prefix' },
            { path: 'configurator', component: ConfiguratorComponent },
            { path: 'dashboard', component: DashboardComponent },
            { path: 'frame-url/:url', component: FrameUrlComponent },
            { path: 'member-gap-list', component: MemberGapListComponent },
            { path: 'member-gap-list/:memberId', component: MemberGapListComponent },
            { path: 'member-care-gap-list', component: MemberCareGapListComponent },
            { path: 'member-gap/:gapId/:memberId', component: MemberGapInfoComponent },
            { path: 'programcreator', component: ProgramcreatorComponent},
            { path: 'quality-central', component: QualityCentralComponent },
            { path: 'measureworklist', component: MeasureworklistComponent},
            { path: 'measureworklist', component: MeasureworklistComponent},
            { path: 'measurelibrary', component: MeasurelibraryComponent},
            { path: 'measurelibrary/:type/:value', component: MeasurelibraryComponent},
            { path: 'member-list', component: MemberListComponent},
            { path: 'measurecreator', component: MeasurecreatorComponent},
            { path: 'measurecreator/:measureId/:type', component: MeasurecreatorComponent},
            { path: 'spv/:memberId', component: SpvComponent},
            { path: 'usersettings', component: UserSettingComponent}
        ]
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class LayoutRoutingModule {}
