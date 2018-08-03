import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../shared/services/http-error-handler.service';
import { MeasurecreatorComponent } from './measure-creator.component';

const httpOptions = {
  headers: new HttpHeaders({
    'Access-Control-Allow-Origin': '*',
    'Content-Type':  'application/json'
  })
};


@Injectable({
  providedIn: 'root'
})
export class MeasurecreatorService {


  private handleError: HandleError;

  constructor(private http: HttpClient,
    httpErrorHandler: HttpErrorHandler) {
      this.handleError =  httpErrorHandler.createHandleError('MeasurecreatorService');
     }


  MeasurecreatorSubmit(model: any): Observable<any> {
        console.log('MeasurecreatorClick');

        return this.http.post('http://10.6.122.180:8080/curis/qms/createProgram', model, httpOptions)
        . pipe(
            catchError(this.handleError('MeasurecreatorSubmit', model))
          );
       // this.http.post('http://<hostname>:<port>/curis/qms/createProgram',model);
  }

  getMeasuerInfo(measureId) {
    return this.http.get(`http://healthinsight:8082/curis/qms/measure_list/${measureId}`);
  }
}
