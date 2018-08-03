import { Component, OnInit, Input, ElementRef } from '@angular/core';
import { Replace } from '../../../shared/index';
@Component({
    selector: 'app-header',
    templateUrl: './header.component.html',
    styleUrls: ['./header.component.scss']
})
export class HeaderComponent implements OnInit {
    @Input() fixed: boolean;

    @Input() navbarBrand: any;
    @Input() navbarBrandFull: any;
    @Input() navbarBrandMinimized: any;

    @Input() sidebarToggler: any;
    @Input() mobileSidebarToggler: any;

    @Input() asideMenuToggler: any;
    @Input() mobileAsideMenuToggler: any;

    constructor(private el: ElementRef) {}

    ngOnInit() {
        Replace(this.el);
        this.isFixed(this.fixed);
    }

    isFixed(fixed: boolean): void {
        if (this.fixed) { document.querySelector('body').classList.add('header-fixed'); }
    }

    imgSrc(brand: any): void {
        return brand.src ? brand.src : '';
    }

    imgWidth(brand: any): void {
        return brand.width ? brand.width : 'auto';
    }

    imgHeight(brand: any): void {
        return brand.height ? brand.height : 'auto';
    }

    imgAlt(brand: any): void {
        return brand.alt ? brand.alt : '';
    }

    breakpoint(breakpoint: any): void {
        console.log(breakpoint);
        return breakpoint ? breakpoint : '';
    }
}
