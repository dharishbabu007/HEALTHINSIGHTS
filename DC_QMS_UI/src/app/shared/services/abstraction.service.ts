import { CachedHttpClient } from './cache-httpclient';
import { Injectable } from '@angular/core';
import { environment } from '../../../environments/environment';
import { Observable,throwError } from 'rxjs';
import {  filter, map, catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../shared/services/http-error-handler.service';
import { PatScreen } from '../../layout/pat-screen/pat-screen.component';
import { Intervention } from '../../layout/health/interventions/interventions.component';
import { HttpClient, HttpHeaders } from '@angular/common/http';

const httpOptions = {
    headers: new HttpHeaders({
      'Access-Control-Allow-Origin': '*',
      'Content-Type':  'application/json'
    })
  };
  @Injectable()
export class AbstractionService {
    private handleError: HandleError;
    constructor(private http: CachedHttpClient,  httpErrorHandler: HttpErrorHandler, private http1: HttpClient) {
        this.handleError =  httpErrorHandler.createHandleError();
    }

    getMemberDetails(memberID) {
        return this.http.get(`http://localhost:8082/curis/chart_abstraction/searchPatient/${memberID}`);
    }
    getPatient(memberID){
        return this.http.get(`http://localhost:8082/curis/chart_abstraction/getPatientDetails/${memberID}`);  
    }
    getVisit(memberID){
        return this.http.get(`http://localhost:8082/curis/chart_abstraction/getChartAbsVisits/${memberID}`);  
    }
    getEncounterTypes(){
        return this.http.get(`http://localhost:8082/curis/chart_abstraction/getEncounterTypes`);  
    }
    getpatientDetailForm(memberID,visitID,chartType){
        return this.http.get(`http://localhost:8082/curis/chart_abstraction/getChartAbs/${memberID}/${visitID}/${chartType}`);  
    }
    getpatientDetailHistoryForm(memberID,chartType){
        return this.http.get(`http://localhost:8082/curis/chart_abstraction/getChartAbsHistory/${memberID}/${chartType}`);  
    }
    addVisitDetails(visitDetails){
        //console.log(visitDetails);
        return this.http.post(`http://localhost:8082/curis/chart_abstraction/addChartAbs`,visitDetails);  
    }
    
    getAllergiesList(tabSec, tabTxtVal) {
        return this.http.get(`http://localhost:8082/curis/qms/dropdown_list/${tabSec}/${tabTxtVal}`);
    }
    submitTabData(totalTabData) {
        return this.http.post(`http://localhost:8082/curis/chart_abstraction/addChartAbs`,totalTabData); 
    }
    getStatus(){
        return this.http.get(`http://localhost:8082/curis/qms/dropdown_list/Ref_Relation_Status/Relation_Status`)
    }
    getProviderNames(){
        return this.http.get(`http://localhost:8082/curis/qms/dropdown_namevalue_list/DIM_PROVIDER/PROV_ID/EXTERNAL_NAME`)
    }

}