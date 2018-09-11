import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../shared/services/http-error-handler.service';
import { Filemanager } from './file-manager.component';


const httpOptions = {
  headers: new HttpHeaders({
    'Access-Control-Allow-Origin': '*',
    'Content-Type':  'application/json'
  })
};

 
@Injectable({
  providedIn: 'root'
})
export class FileManagerService {

    headers={
        headers: new HttpHeaders({
          'Content-Type':  'application/json',
          'Authorization': 'my-auth-token'
        })
       }

  private handleError: HandleError;

  constructor(private http: HttpClient,
    httpErrorHandler: HttpErrorHandler,
   ) {
      this.handleError =  httpErrorHandler.createHandleError('FileManagerService');
     }

 
 Upload(model: Filemanager,name): Observable<Filemanager> {
        console.log(name);

        return this.http.post<Filemanager>('http://healthinsight:8082/curis/qms_file/import',model)
        . pipe(
            catchError(this.handleError('Upload', model))
          );

       // this.http.post('http://<hostname>:<port>/curis/qms/createProgram',model);
  }

  Run(selectedModel): Observable<Filemanager>{
    return this.http.get<Filemanager>(`http://healthinsight:8082/curis/qms_file/run_r/${selectedModel}`)
        . pipe(
            catchError(this.handleError('run Failed'))
          );
  }

}
