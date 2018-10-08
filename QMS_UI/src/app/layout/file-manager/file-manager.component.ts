import { Component, OnInit, LOCALE_ID  } from '@angular/core';
import { DatePipe } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { GapsService } from '../../shared/services/gaps.service';
import { MessageService } from '../../shared/services/message.service';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { Router } from '@angular/router';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Filemanager } from './file-manager.component';
import { FileManagerService } from './file-manager.service';


import {SelectItem} from 'primeng/api';
import { Capabilities } from '../../../../node_modules/protractor';


//import { formDirectiveProvider } from '../../../../node_modules/@angular/forms/src/directives/reactive_directives/form_group_directive';

@Component({
  selector: 'app-fileManager',
  templateUrl: './file-manager.component.html',
  styleUrls: ['./file-manager.component.scss'],
  providers: [GapsService]
})
export class FileManagerComponent implements OnInit {

  cars: SelectItem[];

  selectedModel: string = null;
  some:Response;
  public myForm: FormGroup;
  SelectedFile: File = null;
  Uploded: boolean;
  constructor(private _fb: FormBuilder,
    private gapsService: GapsService,
    private msgService: MessageService,
    private router: Router,
    private route: ActivatedRoute,
    private http: HttpClient,
    private messageService: MessageService,
    private FileManagerService: FileManagerService
    ) {
      this.route.params.subscribe(params => {
        this.selectedModel = params['selectedModel'];
    });

        this.myForm = this._fb.group({
         
        });

        this.cars = [
          {label: 'No Show Appointment prediction', value: 'noShow'},
          {label: 'Model 1', value: 'model1'},
          {label: 'Model 2', value: 'model2'}
        
      ];

     

    }

handleFileInput(event) {
console.log(event);
this.SelectedFile= <File>event.target.files[0];

}
//uploadFileToActivity() {
 //   this.fileUploadService.postFile(this.fileToUpload).subscribe(data => {
      // do something, if upload success
   //   }, error => {
   //     console.log(error);
    //  });
 // }
 ngOnInit() {
 
  }
//onUpload(){

//if(this.clicked){

//const fd = new FormData();
//fd.append('file', this.SelectedFile);

//console.log(fd);

  //this.http.post('http://healthinsight:8082/curis/qms_file/import', fd, this.headers).subscribe();
  //console.log(this.res)

  //this.msgService.success('File Uploaded');
  //}
  //else{
    //this.msgService.error("Please Select a model");
  //}
//}


onUpload() {
  
    const fd = new FormData();
    fd.append('file', this.SelectedFile);

    if(this.SelectedFile!= null){
console.log(this.SelectedFile.name);
this.FileManagerService.Upload(fd,this.SelectedFile.name).subscribe((res: any) => {
  if (res.status === 'SUCCESS') {
    this.msgService.success('file Upload Successfully');
    this.Uploded = true;
  } else {
    this.msgService.error(res.message);
  }
} );


console.log(this.Uploded)
    }
    else{
      this.Uploded = false;
      this.msgService.error("Please Select a file");
    }


}

onRun(){

  if(this.selectedModel!=null && this.Uploded==true){
    console.log(this.selectedModel);
  this.FileManagerService.Run(this.selectedModel).subscribe( event => console.log(event));
}
else if( this.Uploded==true && this.selectedModel==null){
  this.msgService.error("Please Select a model");
}
else{
  this.msgService.error("Please upload a file");
}

}

TestRun(){

  if(this.selectedModel!=null && this.Uploded==true){
  
    this.router.navigateByUrl('/csv1');
}
else if( this.Uploded==true && this.selectedModel==null){
  this.msgService.error("Please Select a model");
}
else{
  this.msgService.error("Please upload a file");
}
}

}
export interface Filemanager {
 
   
   }
