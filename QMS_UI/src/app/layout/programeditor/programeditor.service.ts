import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../shared/services/http-error-handler.service';
import { ProgramCreator } from './programeditor.component';


const httpOptions = {
  headers: new HttpHeaders({
    'Access-Control-Allow-Origin': '*',
    'Content-Type': 'application/json'
  })
};


@Injectable({
  providedIn: 'root'
})
export class ProgrameditorService {


  private handleError: HandleError;

  constructor(private http: HttpClient,
    httpErrorHandler: HttpErrorHandler) {
      this.handleError =  httpErrorHandler.createHandleError('ProgrameditorService');
     }
  programCreatorSubmit(model: ProgramCreator): Observable<ProgramCreator> {
        console.log('ProgramCreator');

        return this.http.post<ProgramCreator>('http://healthinsight:8082/curis/program/editProgram', model, httpOptions)
        . pipe(
           catchError(this.handleError('programCreatorSubmit', model))
         );
       // this.http.post('http://<hostname>:<port>/curis/qms/createProgram',model);
  }
  getProgramNames(){
    return this.http.get(`http://healthinsight:8082/curis/qms/dropdown_list/QMS_QUALITY_PROGRAM/PROGRAM_NAME`);
  }
  getProgramDetails(name){
    return this.http.get(`http://healthinsight:8082/curis/program/getProgramByName/${name}`)
  }

}
