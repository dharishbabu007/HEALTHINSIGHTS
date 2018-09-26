import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { ProgrameditorComponent } from './programeditor.component';
import { CommonModule } from '@angular/common';
import { NgbCarouselModule, NgbAlertModule } from '@ng-bootstrap/ng-bootstrap';


const routes: Routes = [
    {
        path: '', component: ProgrameditorComponent
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class DashboardRoutingModule {
}
export class ProgrameditorRoutingModule {
}
