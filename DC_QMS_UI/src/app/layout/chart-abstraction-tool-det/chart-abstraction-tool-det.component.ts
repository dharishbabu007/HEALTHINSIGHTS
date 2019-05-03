import { Component, OnInit } from '@angular/core';
import { AbstractionService } from '../../shared/services/abstraction.service';
import { debug } from 'util';


@Component({
  selector: 'app-chart-abstraction-tool-det',
  templateUrl: './chart-abstraction-tool-det.component.html',
  styleUrls: ['./chart-abstraction-tool-det.component.scss']
})
export class ChartAbstractionToolDetComponent implements OnInit {
  displayconfirmBox:boolean = false;
  message:String;
  memberInfo:any;
  memberID:any;
  visitId:any;
  patientDetail:any = {};
  patientHistoryData: any = [];
  chartType:any;
  memberList:any;
  addedAllergy:any;
  addedMedication:any;
  addedImmunization:any;
  addedDisease:any;
  addedSubstance:any;
  addedSdoh:any;
  addedFamilyhistory:any;
  visit:any;
  Patients:any;
  visitPatients:any;
  statusList:any;
  /* allergies:any;
  medications:any;
  immunizations:any;
  diseases:any;
  sdohs:any;
  substances:any;
  familyhistorys:any; */
  selectModelfamily:any = {};
  constructor(private data: AbstractionService) { }

  ngOnInit() {
   this.memberInfo = JSON.parse(localStorage.getItem("memberInfo"));
   this.memberID =  this.memberInfo.memberID;
   

   this.visitId = this.memberInfo.visitId;
   if(this.visitId == undefined) {
     this.visitId = 0;
   }
   this.data.getpatientDetailForm(this.memberID, this.visitId, 'ALLERGIES').subscribe((data: any) => {
    this.allergies  = data;
  });
  this.data.getpatientDetailForm(this.memberID, this.visitId, 'MEDICATIONS').subscribe((data: any) => {
    this.medications  = data;
      
  });
  this.data.getpatientDetailForm(this.memberID, this.visitId, 'IMMUNIZATIONS').subscribe((data: any) => {
    this.immunizations  = data;
      
  });
  this.data.getpatientDetailForm(this.memberID, this.visitId, 'DISEASE').subscribe((data: any) => {
    this.diseases  = data;
      
  });
  this.data.getpatientDetailForm(this.memberID, this.visitId, 'SDOH').subscribe((data: any) => {
    this.sdohs  = data;
      
  });
  this.data.getpatientDetailForm(this.memberID, this.visitId, 'SUBSTANCE').subscribe((data: any) => {
    this.substances  = data;
      
  });
  this.data.getpatientDetailForm(this.memberID, this.visitId, 'FAMILYHISTORY').subscribe((data: any) => {
    this.familyhistorys  = data;
      
  });

  this.data.getpatientDetailHistoryForm(this.memberID, 'ALLERGIES').subscribe((data: any) => {
    this.patientHistoryData  = data;
      
  });
  
  //  this.data.getpatientDetailForm(this.memberID, this.visitId, 'VISIT').subscribe((data: any) => {
  //       this.patientDetail  = data[0];
            
                
  //   });
  this.data.getPatient(this.memberID).subscribe((data: any) => {
    this.patientDetail = data;
    });
    this.data.getStatus().subscribe((res:any)=>{
      this.statusList =[];
      res.forEach(element => {
        this.statusList.push({label:element,value:element})
      });
      
    })
  }

allergies = [{"allergiesDrug": "", "allergiesReaction": "", "allergiesStartDate": "", "allergiesEndDate": "", "allergiesNotes":"", "memberId": this.memberID, "visit": 0, "chartType": ""}];
medications = [{"medicationsNdcDrugCodeName": "", "medicationsDaysSupplied":"", "medicationsQuantity": "", "medicationsNotes": "", "medicationsPrescriptionDate": "", "medicationsStartDate": "","medicationsDispensedDate": "", "medicationsEndDate": "","medicationsProviderNpi" :"", "medicationsPharmacyNpi" : "", "memberId": this.memberID, "visit": 0, "chartType": ""}];
immunizations = [{"immunizationsName": "", "immunizationsStartDate": "", "immunizationsEndDate": "", "immunizationsNotes": "", "memberId": this.memberID, "visit": 0, "chartType": ""}];
diseases = [{"disease" : "", "diseaseDiagnosisDate": "", "diseaseEndDate" : "", "diseaseNotes": "", "memberId": this.memberID, "visit": 0, "chartType": ""}];
sdohs = [{"sdohCategory" : "", "sdohProblem": "", "sdohNotes": "", "memberId": this.memberID, "visit": 0, "chartType": ""}];

substances = [{"substanceUse" : "", "substanceFrequency": "", "substanceNotes": "", "memberId": this.memberID, "visit": 0, "chartType": ""}];
familyhistorys = [{"familyHistoryRelation" : "", "familyHistoryStatus": "", "familyHistoryDisease": "", "familyHistoryNotes": "", "memberId": this.memberID, "visit": 0, "chartType": ""}];


totalTabData:any = [];

familystatus:any = ["Alive", "Deceased"];
selectedFamilyStatus:string = "Alive";
search(event, tabSec, tabTxtVal) {
     // console.log(event.query);
    this.data.getAllergiesList(tabSec, tabTxtVal).subscribe((data: any) => {
        this.memberList = data;
        /* data.forEach(element => {
          this.memberList.push(`${element.firstName} ${element.lastName} [${element.memberId}]`);
        }); */
        /*data.forEach(element => {
            this.memberList.push(`${element}`);
            
        });*/
    });
}




handleChange(e) {
    var index = e.index;
    if(index == 0) {
      this.chartType = 'ALLERGIES'; 
      //this.totalTabData.push(this.allergies[0]);
      
    }
    else if(index == 1) {
      this.chartType = 'MEDICATIONS';
      
      //this.totalTabData.push(this.medications[0]);
    }
    else if(index == 2) {
      this.chartType = 'IMMUNIZATIONS';
      
      //this.totalTabData.push(this.immunizations[0]);
    }
    else if(index == 3) {
      this.chartType = 'DISEASE';
      
      //this.totalTabData.push(this.diseases[0]);

    }
    else if(index == 4) {
      this.chartType = 'SDOH';
      
      //this.totalTabData.push(this.sdohs[0]);
    }
    else if(index == 5) {
      this.chartType = 'SUBSTANCE';
      
      //this.totalTabData.push(this.substances[0]);
    }
    else if(index == 6) {
      this.chartType = 'FAMILYHISTORY';
      
      //this.totalTabData.push(this.familyhistorys[0]);
    }
    
    this.data.getpatientDetailHistoryForm(this.memberID, this.chartType).subscribe((data: any) => {
      this.patientHistoryData  = data;
        
    });

    
}
addRow(chartType) {
  if(chartType == 'allergy') {
    this.addedAllergy = {};
    this.addedAllergy.allergiesDrug = "";
  //  console.log(this.addedAllergy);
    this.allergies.push(this.addedAllergy);
    console.log("allergies", this.allergies);
  }
  else if(chartType=='medication') {
    this.addedMedication = [];
    this.addedMedication.medicationsNdcDrugCodeName = "";
    this.medications.push(this.addedMedication);
    console.log("medications", this.medications);
  }
  else if(chartType=='immunization') {
    this.addedImmunization = [];
    this.addedImmunization.immunizationsName = "";
    this.immunizations.push(this.addedImmunization);
    console.log("immunizations", this.immunizations);
  }
  else if(chartType=='disease') {
    this.addedDisease = [];
    this.addedDisease.disease = "";
    this.diseases.push(this.addedDisease);
    console.log("diseases", this.diseases);
  }
  else if(chartType=='sdoh') {
    this.addedSdoh = [];
    this.addedSdoh.sdohCategory = "";
    this.sdohs.push(this.addedSdoh);
    console.log("sdohs", this.sdohs);
  }
  else if(chartType=='substance') {
    this.addedSubstance = [];
    this.addedSubstance.substanceUse = "";
    this.substances.push(this.addedSubstance);
    console.log("substances", this.substances);
  }
  else if(chartType=='familyhistory') {
    this.addedFamilyhistory = {};
    this.addedFamilyhistory.familyHistoryRelation = "";
    this.familyhistorys.push(this.addedFamilyhistory);
    console.log("familyhistorys", this.familyhistorys);
  }
  
}
submitChartToolData() {
 console.log(this.allergies);
  for(var i=0; i<this.allergies.length; i++ ){
    this.allergies[i].memberId = this.memberID;
    this.allergies[i].visit = this.visitId;
    this.allergies[i].chartType = "ALLERGIES";
    
    this.totalTabData.push(this.allergies[i]);
    
  }
  // debugger;
  for(var i=0; i<this.medications.length; i++ ){
    this.medications[i].memberId = this.memberID;
    this.medications[i].visit = this.visitId;
    this.medications[i].chartType = "MEDICATIONS";
    this.totalTabData.push(this.medications[i]);
  }
  for(var i=0; i<this.immunizations.length; i++ ){
    this.immunizations[i].memberId = this.memberID;
    this.immunizations[i].visit = this.visitId;
    this.immunizations[i].chartType = "IMMUNIZATIONS";
    this.totalTabData.push(this.immunizations[i]);
  }
  for(var i=0; i<this.diseases.length; i++ ){
    this.diseases[i].memberId = this.memberID;
    this.diseases[i].visit = this.visitId;
    this.diseases[i].chartType = "DISEASE";
    this.totalTabData.push(this.diseases[i]);
  }
  for(var i=0; i<this.sdohs.length; i++ ){
    this.sdohs[i].memberId = this.memberID;
    this.sdohs[i].visit = this.visitId;
    this.sdohs[i].chartType = "SDOH";
    this.totalTabData.push(this.sdohs[i]);
  }
  for(var i=0; i<this.substances.length; i++ ){
    this.substances[i].memberId = this.memberID;
    this.substances[i].visit = this.visitId;
    this.substances[i].chartType = "SUBSTANCE";
    this.totalTabData.push(this.substances[i]);
  }
  for(var i=0; i<this.familyhistorys.length; i++ ){
    this.familyhistorys[i].memberId = this.memberID;
    this.familyhistorys[i].visit = this.visitId;
    this.familyhistorys[i].chartType = "FAMILYHISTORY";
    this.totalTabData.push(this.familyhistorys[i]);
  }
  
  
 console.log(this.totalTabData)
  
  
  
  
  
  
  
  this.data.submitTabData(this.totalTabData).subscribe((data: any) => {
        this.displayconfirmBox = false;
    //    console.log("data", data);
        
  });
  
}
showSaveDialog(){
  this.displayconfirmBox = true;
}

hideSaveDialog(){
  this.displayconfirmBox = false;
}
loadPatientInfo(memberString) {
    const pattern = /\[(.*?)\]/g;
    const match = pattern.exec(memberString);
    //console.log(match, '-', memberString);
    if (match.length) {
        this.memberID = match[1];
        this.data.getPatient(this.memberID).subscribe((data: any) => {
            //console.log(data);
            /*data.qmsGicLifecycle.forEach(element => {
                element.measureTitle = element.factHedisGapsInCare[0].measureTitle;
            });*/
            this.Patients = data;
            this.data.getVisit(this.memberID).subscribe((data: any) => {
               // console.log(data);
                /*data.qmsGicLifecycle.forEach(element => {
                    element.measureTitle = element.factHedisGapsInCare[0].measureTitle;
                });*/
                this.visitPatients = data;
                console.log( this.visitPatients);
            });    
        });
    }
}

}
