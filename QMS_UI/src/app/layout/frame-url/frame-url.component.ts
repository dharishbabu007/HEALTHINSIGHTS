import { Component, OnInit } from '@angular/core';
import { SafePipe } from '../../app.component';
import { ActivatedRoute } from '@angular/router';
@Component({
    selector: 'app-frame-url',
    templateUrl: './frame-url.component.html',
    styleUrls: ['./frame-url.component.scss'],
    providers: [ SafePipe ]
})
export class FrameUrlComponent implements OnInit {
    public externalURL: any;

    constructor(private route: ActivatedRoute, private safe: SafePipe) {
        // console.log(this.route);
        this.route.params.subscribe(params => {
            console.log(params['url']);
            this.externalURL = this.safe.transform(params['url']);
        });
    }

    ngOnInit() {}

}
