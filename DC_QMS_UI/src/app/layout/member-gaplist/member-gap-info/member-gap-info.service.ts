import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../../shared/services/http-error-handler.service';
import { MemberGap } from './member-gap-info.component';


// const httpOptions = {
//   headers: new HttpHeaders({
//     'Access-Control-Allow-Origin': '*',
//     'Content-Type':  'application/json'
//   })
// };

 
@Injectable({
  providedIn: 'root'
})
export class MemberGapService {

    // headers={
    //     headers: new HttpHeaders({
    //       'Content-Type':  'application/json',
    //       'Authorization': 'my-auth-token'
    //     })
    //    }
    //    headers1={
    //     headers: new HttpHeaders({
    //       'responseType': 'blob',
    //       'Authorization': 'my-auth-token'
    //     })
    //    }
  private handleError: HandleError;

  constructor(private http: HttpClient,
    httpErrorHandler: HttpErrorHandler,
   ) {
      this.handleError =  httpErrorHandler.createHandleError('MemberGapService');
     }

 
     uploadCloseGapFiles(model: MemberGap): Observable<MemberGap> {

        return this.http.post<MemberGap>('http://localhost:8082/curis/closeGaps/gic_lifecycle_import/',model)
        . pipe(
          catchError(this.handleError('Logged in user data is null. Please logout and login once.'))
          );

       // this.http.post('http://<hostname>:<port>/curis/qms/createProgram',model);
  }
  commonfileupload(type,model: MemberGap): Observable<MemberGap> {
    return this.http.post<MemberGap>(`http://localhost:8082/curis/closeGaps/file_upload/${type}`,model). pipe(
      catchError(this.handleError('Logged in user data is null. Please logout and login once.'))
      );
  }
  downloadfile(link){
    return this.http.get(`http://localhost:8082/curis/closeGaps/fileDownload?filePath=${link}`,{responseType: 'text'}
    ). pipe(
    catchError(this.handleError('Upload', link)),
  )}
  downloadimage(link){
    return this.http.get(`http://localhost:8082/curis/closeGaps/fileDownload?filePath=${link}`,{responseType: 'blob'}
    ). pipe(
    catchError(this.handleError('Upload', link)),
  )}

}
