import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { SafePipe } from '../../app.component';
import { ActivatedRoute } from '@angular/router';
import { MemberCareGaps } from '../../shared/services/gaps.data';
import { GapsService } from '../../shared/services/gaps.service';


@Component({
    selector: 'app-tables',
    templateUrl: './like-hood-churn.component.html',
    styleUrls: ['./like-hood-churn.component.scss'],
    animations: [routerTransition()],
    providers: [ SafePipe ,GapsService ]
})
export class LikelihoodToChurnComponent implements OnInit {
    public externalURL: any;
    title: any;
    type: any;
    constructor(private route: ActivatedRoute, private safe: SafePipe, private gapsService: GapsService) {
         // console.log(this.route);
         this.route.params.subscribe(params => {
            this.type = params['type'];
            this.title = (this.type === '1' ) ? 'Likelihood To Churn' : 'Likelihood To Enroll';
        });
        this.externalURL = this.safe.transform('http://192.168.184.70/t/CurisSite/views/Churn-MemberList/MemberListDash?:embed=y&:showAppBanner=false&:showShareOptions=true&:display_count=no&:showVizHome=no');
       
    }

    membergaps: MemberCareGaps[];
    membergaps1: MemberCareGaps[];
    cols: any[];
    cols1:any[];
    loading = true;
     genderTypes =  [
        { label: 'Select', value: '' },
        { label: 'Male', value: 'Male' },
        { label: 'Female', value: 'Female' }
    ];
    ngOnInit() {
        this.gapsService.getLikelihoodMeasureDetails().subscribe((data: MemberCareGaps[]) => {
            data.forEach(element => {
                element.age = parseInt(element.age, 10);
            });
            this.membergaps = data;
            this.loading = false;
        });
        this.cols = [
            { field: 'memberId', header: 'Member Id' },
            { field: 'memberName', header: 'Member Name' },
            { field: 'enrollGaps', header: 'Enrollment Gaps' },
            { field: 'outOfPocketExpenses', header: 'Out of Pocket Expenses' },
            { field: 'utilizerCategory', header: 'Utilizer Category' },
            { field: 'age', header: 'Age' },
            { field: 'amountSpend', header: 'Amount Spend' },
            { field: 'er', header: 'ER Visits' },
            { field: 'reasonNotEnroll', header: 'Reason to not Enroll' },
            { field: 'likeliHoodEnroll', header: 'Likelihood to Enroll' },
            { field: 'enrollmentBin', header: 'Enrollment Potential' },
            
           
        ];
        this.gapsService.getLikehoodchurnMeasureDetails().subscribe((data: MemberCareGaps[]) => {
            this.membergaps1 = data;
            this.loading = false; 
           
        });
        this.cols1 = [
            { field: 'memberId1', header: 'Member Id' },
            { field: 'memberName1', header: 'What persona does the member belong to?'},
            { field: 'memberName1', header: 'What are your motivations for leading a healthy life?' },
            { field: 'Utilization', header: 'Why would you abstain from enrolling into the wellness program?' },
            { field: 'Channel', header: 'How many gaps did the member have during the enrollment period?' },
            { field: 'Reward', header: 'What is the participation frequency for the member?' },
            { field: 'Reward', header: 'Likelihood to Churn'}
           
        ];
        
    }
}