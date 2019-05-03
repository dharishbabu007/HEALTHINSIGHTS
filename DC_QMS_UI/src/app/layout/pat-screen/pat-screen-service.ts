import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders,HttpErrorResponse  } from '@angular/common/http';
import { Observable,of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { HandleError } from '../../shared/services/http-error-handler.service';
import { PatScreen } from './pat-screen.component';
import { MessageService } from '../../shared/services/message.service';
@Injectable({
  providedIn: 'root'
})
export class PatScreenService {
  private handleError: HandleError;

  constructor(private http: HttpClient,private MessageService:MessageService
   ) {
     }
     private handleAuthError(err: HttpErrorResponse): Observable<any> {
      //handle your auth error or rethrow
      if (err.status === 401) {
        //navigate /delete cookies or whatever
        console.log('handled error ' + err.status);
        // if you've caught / handled the error, you don't want to rethrow it unless you also want downstream consumers to have to handle it as well.
        return of(err.message);
      }
      throw err;
    }
  commonfileupload(type,model: PatScreen): Observable<PatScreen> {
    return this.http.post<PatScreen>(`http://localhost:8082/curis/closeGaps/file_upload/${type}`,model) . pipe(catchError((error, caught) => {
      //intercept the respons error and displace it to the console
      this.MessageService.error(error.error.message);
      this.handleAuthError(error.error.message);
      return of(error);
    }) as any);
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
  getCodeType(measureId,valueset){
    return this.http.get(`http://localhost:8082/curis/pat/action_on_caregap_Code_Type_list/${measureId}/${valueset}`)
  }
  getCodeList(measureId,valueset,codeType){
    return this.http.get(`http://localhost:8082/curis/pat/action_on_caregap_Codes_list/${measureId}/${valueset}/${codeType}`)
  }
}
