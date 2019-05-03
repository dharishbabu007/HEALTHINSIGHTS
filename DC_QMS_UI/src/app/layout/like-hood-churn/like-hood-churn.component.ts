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

    membergapslhe: MemberCareGaps[];
    membergapslhc: MemberCareGaps[];
    cols: any[];
    cols1:any[];
    loading = true;
    loadinglhe = true;
     genderTypes =  [
        { label: 'Select', value: '' },
        { label: 'Male', value: 'Male' },
        { label: 'Female', value: 'Female' }
    ];
    ngOnInit() {
        if(this.type == 2){
        this.gapsService.getlheMeasureDetails().subscribe((data: MemberCareGaps[]) => {
            data.forEach(element => {
                element.age = parseInt(element.age, 10);
            });
            this.membergapslhe = data;
            this.loadinglhe = false;
        });
        this.cols = [
            { field: 'memberId', header: 'Member Id' },
            { field: 'memberName', header: 'Member Name' },
            { field: 'age', header: 'Age' },
            { field: 'gender', header: 'Gender' },
            { field: 'lengthTimeUninsured', header: 'Length of time uninsured' },
            { field: 'outOfPocketExpenses', header: 'Out of Pocket Expenses' },
            { field: 'amountSpend', header: 'Amount Spend' },
            { field: 'daysPendingTermination', header: 'Days Pending for Termination' },
            { field: 'comorbidityCount', header: 'Comorbidity Count' },
            { field: 'reasonNotEnroll', header: 'Predicted Reason to not enroll' },
            { field: 'likeliHoodEnroll', header: 'Likelihood to Enroll' },
            { field: 'enrollmentBin', header: 'Enrollment' },
            
           
        ];
    }
        else{
        this.gapsService.getLikehoodchurnMeasureDetails().subscribe((data: MemberCareGaps[]) => {
            this.membergapslhc = data;
            this.loading = false; 
           
        });
        this.cols1 = [
            { field: 'memberID', header: 'Member Id' },
            { field: 'memberName', header: 'Member Name' },
            { field: 'age', header: 'Age' },
            { field: 'gender', header: 'Gender' },
            // {field:'persona',header:'Persona'},
            { field: 'participationQuotient', header: 'Participation Quotient' },
            // { field: 'comorbidityCount', header: 'Comorbidity Count' },
            // { field:'frequencyExercise',header:'Frequency Exercise'},
            // { field:'motivations',header:'Motivations'},
            { field: 'outPocketExpenses', header: 'Out of Pocket Expenses' },
            // { field: 'enrollmentGaps', header: 'Enrollment Gaps' },
            // { field: 'amountSpend', header: 'Amount Spend' },
            { field: 'addictions', header: 'Addictions' },
            { field: 'copd', header: 'COPD' },
            { field: 'occupation', header: 'Occupation' },
            { field: 'likelihoodChurn', header: 'Likelihood to Churn'},
            { field: 'churn', header: 'Predicted  Churn'}
           
        ];
         }
        
    }
}