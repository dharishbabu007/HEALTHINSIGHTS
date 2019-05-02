import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { AbstractionService } from '../../shared/services/abstraction.service';


@Component({
  selector: 'app-chart-abstraction-tool',
  templateUrl: './chart-abstraction-tool.component.html',
  styleUrls: ['./chart-abstraction-tool.component.scss']
})

export class ChartAbstractionToolComponent implements OnInit {
   abstucttemplate:string = 'search';
  constructor(private _router: ActivatedRoute,
    private router: Router,private abstrationService: AbstractionService) { }
    providerList:any;
  ngOnInit() {

  }

  encounterTypes = [ ];
  destination = [];
  memberList: any;
  memberID: string;
  memberName: string;
  Patients:any;
  visitPatients:any;
  noPatients:boolean = false;
  displayDialogBox:boolean = false;
  smallloader:boolean = false;
  dateError:boolean= false;
  visitId: any;
  memberInfo: any;
  PatientDetail:any;
  visitDetails:visitDetails[];
  showDialog(){
    if(this.visitPatients == null) {
        this.visitPatients = {};
        this.visitPatients.provider = "";
    }
    this.abstrationService.getEncounterTypes().subscribe((data: any) => {
        this.smallloader = true;
        this.encounterTypes = [];
        this.encounterTypes = data;
        this.displayDialogBox= true;
        this.smallloader = false;
    });
  }
  hideDialog(){
    this.memberInfo = {};
    this.memberInfo = {"memberID" : this.memberID, "visitId": this.visitId, 'chartType' : 'VISIT'};
    localStorage.setItem("memberInfo", JSON.stringify(this.memberInfo));    
    this.Patients.encounterType = this.destination.join();
    this.displayDialogBox= false;
    this.smallloader = false;
}
backToLogin(blockType) {
    if(blockType == "nodata") {
        this.noPatients = false;
    }
    else if(blockType == "patienthistory") {
        this.noPatients = false;
        this.abstucttemplate = 'search';
    }
    else {
        this.abstucttemplate = 'search';
    }
    this.Patients = null;
    this.memberName = "";
    this.memberID = "";
    
}
  search(event) {
     // console.log(event.query);
    this.abstrationService.getMemberDetails(event.query).subscribe((data: any) => {
        this.memberList = [];
        data.forEach(element => {
            this.memberList.push(`${element.firstName} ${element.lastName} [${element.memberId}]`);
        });
    });
}
loadPatientInfo(memberString) {
    const pattern = /\[(.*?)\]/g;
    const match = pattern.exec(memberString);
    //console.log(match, '-', memberString);
    if (match.length) {
        this.memberID = match[1];
        let memberStringArr = memberString.split("[");
        this.memberName = memberStringArr[0];
        this.abstrationService.getPatient(this.memberID).subscribe((data: any) => {
           
                this.noPatients = false;
                this.Patients = data;
                this.abstrationService.getVisit(this.memberID).subscribe((data: any) => {
                   
                    this.visitPatients = data;
                                      //console.log( this.visitPatients);
                });
          
               
        });
    }
}
searchPatient(){
    if(this.Patients == null){
        this.noPatients = true;
    }
    else{
    this.abstucttemplate = 'Patientlist';
    }
}

navigateRecord(){
    this.visitDetails  = 
        [{memberId : this.Patients.patientId, provider :this.visitPatients.provider, 
            encounterStartDate : this.visitPatients.encounterStartDate,encounterEndDate : this.visitPatients.encounterEndDate
        , encounterCode:this.Patients.encounterType.split(","),encounterValueset:this.Patients.encounterType.split(","),chartType:"VISIT"}]
       ;
   
    this.abstrationService.addVisitDetails(this.visitDetails).subscribe((data: any) => {
        this.router.navigate(["/chart-abstract-det/"]);
            
});
   
}
validateDate(dateEvent){
    this.dateError = false;
    if ((Date.parse(this.visitPatients.encounterStartDate) > Date.parse(this.visitPatients.encounterEndDate))) {
         this.dateError = true;
      
    }
}
addPatientForm(visitId){
    this.abstucttemplate = 'PatientTool';
          this.abstrationService.getProviderNames().subscribe((data:any)=>{
        this.providerList =[];
        data.forEach(element =>{
            this.providerList.push({label:element.name,value:element.value})
        })
      //  console.log(this.providerList)
      });
}
detailPatientForm(visitId){
    //this.abstucttemplate = 'PatientTool';
    this.visitId = visitId;
    this.memberInfo = {};
    this.memberInfo = {"memberID" : this.memberID, "visitId": this.visitId, 'chartType' : 'VISIT'};
    localStorage.setItem("memberInfo", JSON.stringify(this.memberInfo));
    
    this.abstrationService.getpatientDetailForm(this.memberID, this.visitId, 'VISIT').subscribe((data: any) => {
            //console.log(data);
            /*data.qmsGicLifecycle.forEach(element => {
                element.measureTitle = element.factHedisGapsInCare[0].measureTitle;
            });*/
            this.PatientDetail  = data;
            console.log("patient detail", this.PatientDetail);
                
    });
    this.router.navigate(["/chart-abstract-det/"]);
}

}
export interface visitDetails {
    memberId : any;
    provider:any;
    encounterStartDate:any;
    encounterEndDate : any;
    encounterCode : any;
    encounterValueset : any;
    chartType : any;
  }