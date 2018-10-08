import { Component, OnInit } from '@angular/core';
import { GapsService } from '../../shared/services/gaps.service';
import { FormGroup, FormControl, Validators, FormBuilder} from '@angular/forms';
import { Router } from '@angular/router';

import { ActivatedRoute } from '@angular/router';
import { MessageService } from '../../shared/services/message.service';


@Component({
  selector: 'app-create-user',
  templateUrl: './create-user.component.html',
  styleUrls: ['./create-user.component.scss']
})
export class CreateUserComponent implements OnInit {
  public myForm: FormGroup;
  roleList: any[];
  statusList: any[];
  userList: any[];
  constructor(private _fb: FormBuilder,
    private GapsService: GapsService,
    private router: Router,
    private route: ActivatedRoute,
  private msgService: MessageService) { }
  ngOnInit() {
    this.myForm = this._fb.group({
    
        userName: ['',[Validators.required]],
        password: ['',[Validators.required]],
        confirmPassword: ['',[Validators.required]],
        roleName: ['', Validators.required],
        status: ['']
      });
}
onSubmit(){
  
}
}
