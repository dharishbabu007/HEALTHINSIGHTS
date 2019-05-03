import { Component, OnInit } from '@angular/core';
import { SafePipe } from '../../app.component';
import { ActivatedRoute } from '@angular/router';
import { Router } from '@angular/router';
import { HttpClient, HttpHeaders } from '@angular/common/http';

const httpOptions = {
    headers: new HttpHeaders({
        'X-Frame-Options' : 'SAMEORIGIN'
    })
  };

@Component({
    selector: 'app-frame-url',
    templateUrl: './frame-url.component.html',
    styleUrls: ['./frame-url.component.scss'],
    providers: [ SafePipe ]
})
export class RStudioComponent implements OnInit {
    public externalURL: any;

      
    constructor(private route: ActivatedRoute, private safe: SafePipe,private Router: Router) {
        
    }

    ngOnInit() {
       // this.Router.navigateByUrl('http://192.168.184.71:8787/auth-sign-in');
       window.open("http://192.168.184.71:8787/auth-sign-in");
    }

}
