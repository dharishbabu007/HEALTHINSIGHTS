import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../router.animations';

@Component({
    selector: 'app-reset-password',
    templateUrl: './reset-password.component.html',
    styleUrls: ['./reset-password.component.scss'],
    animations: [routerTransition()]
})
export class ResetPasswordComponent implements OnInit {
    constructor() {}

    ngOnInit() {}
}
