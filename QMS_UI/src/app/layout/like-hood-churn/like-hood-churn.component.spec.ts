import { async, ComponentFixture, TestBed } from '@angular/core/testing'
import { RouterTestingModule } from '@angular/router/testing'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { LikelihoodToChurnComponent } from './like-hood-churn.component'

describe('LikelihoodToChurnComponent', () => {
  let component: LikelihoodToChurnComponent
  let fixture: ComponentFixture<LikelihoodToChurnComponent>

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        RouterTestingModule,
        BrowserAnimationsModule,
       ],
       declarations: [LikelihoodToChurnComponent]
    })
    .compileComponents()
  }))

  beforeEach(() => {
    fixture = TestBed.createComponent(LikelihoodToChurnComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
 