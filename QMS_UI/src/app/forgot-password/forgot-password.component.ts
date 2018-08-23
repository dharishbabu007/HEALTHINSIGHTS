import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../router.animations';

@Component({
    selector: 'app-forgot-password',
    templateUrl: './forgot-password.component.html',
    styleUrls: ['./forgot-password.component.scss'],
    animations: [routerTransition()]
})
export class ForgotPasswordComponent implements OnInit {
    constructor() {}

    ngOnInit() {}
}
