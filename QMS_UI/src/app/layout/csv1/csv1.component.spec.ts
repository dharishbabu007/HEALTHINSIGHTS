import { async, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { TableModule } from 'primeng/table';
import { DropdownModule } from 'primeng/dropdown';
import { SliderModule } from 'primeng/slider';
import { FormsModule } from '@angular/forms';
import { Csv1Component } from './csv1.component';
import { PageHeaderModule } from '../../shared/modules/page-header/page-header.module';
import { GapsService } from '../../shared/services/gaps.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
describe('MemberGapListComponent', () => {
  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [  RouterTestingModule, PageHeaderModule, SliderModule,
        FormsModule, TableModule, DropdownModule, HttpClientTestingModule ],
      declarations: [Csv1Component],
      providers: [GapsService]
    })
    .compileComponents();
  }));

  it('should create', () => {
    const fixture = TestBed.createComponent(Csv1Component);
    const component = fixture.componentInstance;
    expect(component).toBeTruthy();
  });
});
