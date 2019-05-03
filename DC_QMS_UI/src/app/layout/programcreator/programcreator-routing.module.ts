import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { ProgramcreatorComponent } from './programcreator.component';
import { CommonModule } from '@angular/common';
import { NgbCarouselModule, NgbAlertModule } from '@ng-bootstrap/ng-bootstrap';


const routes: Routes = [
    {
        path: '', component: ProgramcreatorComponent
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class DashboardRoutingModule {
}
export class ProgramcreatorRoutingModule {
}
