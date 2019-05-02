import { NgModule } from '@angular/core';
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';
import { CommonModule } from '@angular/common';
import { TranslateModule } from '@ngx-translate/core';
import { NgbDropdownModule } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';
import { LayoutRoutingModule } from './layout-routing.module';
import { LayoutComponent } from './layout.component';
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
import { MemberCareGapListComponent } from './member-care-gaplist/member-care-gaplist.component';
import { MemberGapListComponent } from './member-gaplist/member-gaplist.component';
import { MemberGapInfoComponent } from './member-gaplist/member-gap-info/member-gap-info.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { FrameUrlComponent } from './frame-url/frame-url.component';
import { MeasureworklistComponent } from './measureworkilst/measure-worklist.component';
import { MeasurelibraryComponent } from './measurelibrary/measure-library.component';
import { MemberListComponent } from './member-list/member-list.component';
import { MeasurecreatorComponent } from './measurecreator/measure-creator.component';
import { SpvComponent } from './spv/spv.component';
import { ConfiguratorComponent } from './configurator/configurator.component';
import { QualityCentralComponent } from './quality-central/quality-central.component';
import { UserSettingComponent } from './usersettings/userSetting.component';
import { LikelihoodToChurnComponent } from './like-hood-churn/like-hood-churn.component';
import { FileManagerComponent } from './file-manager/file-manager.component';
import { Csv1Component } from './csv1/csv1.component';
import { Csv2Component } from './csv2/csv2.component';
import { NoShowGapListComponent } from './noshowG/noshowG.component';
import { ClusterAnalysisComponent } from './cluster-analysis/cluster-analysis.component';
import { MemberEngageComponent } from './member-engage/member-engage.component';
import { CreatePersonasComponent } from './create-personas/create-personas.component';
import { ClusterStatisticsComponent } from './cluster-statistics/cluster-statistics.component';
import { ViewPersonaComponent } from './view-personas/view-personas.component';
import { PersonaDataComponent } from './persona-data/persona-data.component';
import { LikelihoodStatisticsComponent } from './likelihood-Statistics/likelihood-statistics.component';
import { ClusterMemberListComponent } from './cluster-memberlist/cluster-memberlist.component';
import { SpvNewComponent } from './spv-new/spv-new.component';
import { NonComplianceComponent } from './non-compliance/non-compliance.component';
import { HerniaRepairComponent } from './hernia-repair/hernia-repair.component';
import { CommunicationStatisticsComponent } from  './communication-stats/communication-stats.component';
import { CommunicationToEnrollComponent } from './communication-enroll/communication-enroll.component';
import { PatComponent } from './pat-screen/pat-screen.component';
import { RStudioComponent } from './r-studio/frame-url.component';
import { NcStatsComponent } from './non-compliance-statistics/ncStats.component';
import { PersonaClusteringComponent } from './persona-clustering/persona-clustering.component';
import { PCStatsComponent } from './persona-clustering-statistics/pc-stats.component';

import { PageHeaderModule } from '../shared/modules/page-header/page-header.module';
import { SidebarToggleDirective, AsideToggleDirective, SidebarMinimizeDirective,
    MobileSidebarToggleDirective, SidebarOffCanvasCloseDirective, BrandMinimizeDirective } from '../shared/modules/layout.directive';
import { TableModule } from 'primeng/table';
import { DropdownModule } from 'primeng/dropdown';
import { FileUploadModule } from 'primeng/fileupload';
import { InputTextareaModule } from 'primeng/inputtextarea';
import { SliderModule } from 'primeng/slider';
import { AutoCompleteModule } from 'primeng/autocomplete';
import { PanelModule } from 'primeng/panel';
import { MultiSelectModule } from 'primeng/multiselect';
import {TooltipModule} from 'primeng/tooltip';
import { ProgramcreatorComponent } from './programcreator/programcreator.component';
import { ProgrameditorComponent } from './programeditor/programeditor.component';
import { ReactiveFormsModule } from '@angular/forms';
import { CalendarModule } from 'primeng/calendar';

import {LocationStrategy, HashLocationStrategy} from '@angular/common';
import { HttpClientModule } from '@angular/common/http';

import {ListboxModule} from 'primeng/listbox';
import {ChartModule} from 'primeng/chart';
import { CreateUserComponent } from './create-user/create-user.component';
import { CreateRoleComponent } from './create-role/create-role.component';
import {CheckboxModule} from 'primeng/checkbox';
 
import {TreeModule} from 'primeng/tree';
import {ConfirmDialogModule} from 'primeng/confirmdialog';
import {DialogModule} from 'primeng/dialog';
import { NgIdleKeepaliveModule } from '@ng-idle/keepalive';
import { NgxPermissionsModule } from 'ngx-permissions';
import {AccordionModule} from 'primeng/accordion';
import {TieredMenuModule} from 'primeng/tieredmenu';
import {OverlayPanelModule} from 'primeng/overlaypanel';
import {TabViewModule} from 'primeng/tabview';
import {ProgressBarModule} from 'primeng/progressbar';
import {InputSwitchModule} from 'primeng/inputswitch';
import {  SmvComponent } from './smv/smv.component';
import {ToggleButtonModule} from 'primeng/togglebutton';
import {CarouselModule} from 'primeng/carousel';
import {PickListModule} from 'primeng/picklist';
import { AngularDualListBoxModule } from 'angular-dual-listbox';
import { ChartAbstractionToolComponent } from './chart-abstraction-tool/chart-abstraction-tool.component';
import { ChartAbstractionToolDetComponent } from './chart-abstraction-tool-det/chart-abstraction-tool-det.component';

@NgModule({
    imports: [
        CommonModule,
        LayoutRoutingModule,
        TranslateModule,
        TableModule,
        DropdownModule,
        FileUploadModule,
        PageHeaderModule,
        InputTextareaModule,
        SliderModule,
        ReactiveFormsModule,
        CalendarModule,
        PanelModule,
        MultiSelectModule,
        AutoCompleteModule,
        FormsModule,
        TooltipModule,
        CheckboxModule,
        HttpClientModule,
        ListboxModule,
        ChartModule,
        TreeModule,
        ConfirmDialogModule,
        DialogModule,
        AccordionModule,
        TieredMenuModule,
        OverlayPanelModule,
        ProgressBarModule,
        TabViewModule,
        NgbModule.forRoot(),
        NgbDropdownModule.forRoot(),
        NgIdleKeepaliveModule.forRoot(),
        NgxPermissionsModule.forRoot(),
        InputSwitchModule,
        ToggleButtonModule,
        CarouselModule,
        PickListModule,
        AngularDualListBoxModule
    ],

    declarations: [LayoutComponent,
        BrandMinimizeDirective,
        NavDropdownToggleDirective,
        NavDropdownDirective,
        SidebarToggleDirective,
        AsideToggleDirective,
        SidebarMinimizeDirective,
        MobileSidebarToggleDirective,
        SidebarOffCanvasCloseDirective,
        AsideComponent,
        SidebarComponent,
        SidebarFormComponent,
        SidebarHeaderComponent,
        SidebarFooterComponent,
        SidebarNavComponent,
        SidebarNavDropdownComponent,
        SidebarNavItemComponent,
        SidebarNavLinkComponent,
        SidebarNavTitleComponent,
        SidebarMinimizerComponent,
        HeaderComponent,
        MemberCareGapListComponent,
        MemberGapListComponent,
        DashboardComponent,
        FrameUrlComponent,
        MeasureworklistComponent,
        MeasurelibraryComponent,
        MemberListComponent,
        SpvComponent,
        ConfiguratorComponent,
        QualityCentralComponent,
        MemberGapInfoComponent,
        MeasurecreatorComponent,
        ProgramcreatorComponent,
        ProgrameditorComponent,
        UserSettingComponent,
        FileManagerComponent,
        Csv1Component,
        Csv2Component,
        CreateUserComponent,
        CreateRoleComponent,
        NoShowGapListComponent,
        MemberEngageComponent,
        CreatePersonasComponent,
        ClusterAnalysisComponent,
        LikelihoodToChurnComponent,
        ClusterStatisticsComponent,
        ViewPersonaComponent,
        LikelihoodStatisticsComponent,
        PersonaDataComponent,
        ClusterMemberListComponent,
        SpvNewComponent,
        NonComplianceComponent,
        HerniaRepairComponent,
        CommunicationStatisticsComponent,
        CommunicationToEnrollComponent,
        PatComponent,
        RStudioComponent,
        SmvComponent,
        NcStatsComponent,
        PersonaClusteringComponent,
        PCStatsComponent,
        ChartAbstractionToolComponent,
        ChartAbstractionToolDetComponent],
        
 providers: [{provide: LocationStrategy, useClass: HashLocationStrategy}],
        
   

})
export class LayoutModule {}
