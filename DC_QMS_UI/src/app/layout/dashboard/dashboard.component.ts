import { Component, OnInit } from '@angular/core';
import { routerTransition } from '../../router.animations';
import { GapsService } from '../../shared/services/gaps.service';
import { MessageService } from '../../shared/services/message.service';
import { Router } from '@angular/router';
import { concat } from 'rxjs/operators';

@Component({
    selector: 'app-dashboard',
    templateUrl: './dashboard.component.html',
    styleUrls: ['./dashboard.component.scss'],
    animations: [routerTransition()],
    providers: [GapsService]
})
export class DashboardComponent implements OnInit {
    careGaps:any;
    length:any;
    images:any;
    sourcePages:any;
    pageList:any;
    targetPages:any;
    model:any;
    actualtargetPages:any;
    constructor(    private gapsService: GapsService, private MessageService:MessageService, private Router:Router) {
    }

    ngOnInit() {
        this.gapsService.getHomepageCaregpas().subscribe((data :any)=>{
            this.careGaps = data;
            this.length = this.careGaps.length;
           // console.log(this.careGaps)
        });
        this.images = [];
        this.images.push({source:'assets/images/Slide (1).png', alt:'Description for Image 1', title:'Title 1'});
        this.images.push({source:'assets/images/Slide (2).png', alt:'Description for Image 2', title:'Title 2'});
        this.images.push({source:'assets/images/Slide (3).png', alt:'Description for Image 3', title:'Title 3'});
        var user =  JSON.parse(localStorage.getItem('currentUser'));
        this.gapsService.getscreensforrole(user.roleId).subscribe((res:any)=>{
            this.sourcePages =[];
            res.forEach(element => {
             this.sourcePages.push({label:element.name,value:element.value})
            });
        });
        this.gapsService.getselectedpages(user.roleId).subscribe((res:any)=>{
            if(res){
            this.gapsService.getPageList().subscribe((page:any)=>{
                this.pageList =[];
                this.targetPages =[];
                this.actualtargetPages =[];
                this.pageList = page;
                res.screenPermissions.forEach(element =>{
                    if(element.favourites == "Y"){
                        let temp = this.pageList.filter(data => data.value == element.screenId);
                        this.targetPages.push({label:temp[0].name,value:temp[0].value});
                        this.actualtargetPages.push({label:temp[0].name,value:temp[0].value});
                    }
                })
                for(let i=0; i<this.targetPages.length;i++){
                this.sourcePages = this.sourcePages.filter(element => element.value !== this.targetPages[i].value)
                }
            });

        }
        });
       
    }
    displayDialogBox:boolean = false;
    showDialog(){
        this.displayDialogBox= true;
    }
    saveChanges(pages){
 
        if(pages.length >4){
            this.MessageService.error("please select only four pages")
        }
        else{
            this.displayDialogBox= false;
            this.model = [];
            var user =  JSON.parse(localStorage.getItem('currentUser'));
              let temp =[];
            for(let i=0;i<pages.length;i++){
                temp.push({screenId:pages[i].value,favourites:"Y"})
            }
            this.model ={
                "roleId":user.roleId,
                "screenPermissions":temp
            }
            this.gapsService.updateQuickLink(this.model).subscribe((res:any)=>{
                if(res.status =="SUCCESS"){
                    this.MessageService.success("Quick Links Updated Successful");
                    window.location.reload();
                }
                else{
                  this.MessageService.warning(res.error.message)
                }
            });

        }
  
    }
    quickLink(event){
        this.Router.navigate([event]);
    }
    disableOnTargetLength:boolean = false
    onTargetChange:boolean = false
    movedElements(event){
     //   console.log(event);
       // console.log(this.targetPages.length);
        if(this.targetPages.length>4){
            this.disableOnTargetLength = true;
            this.onTargetChange = true;
        }
        else{
            this.disableOnTargetLength = false;
            this.onTargetChange = false;
        }
    }
    movedElementsToSource(){
        if(this.targetPages.length>4){
            this.disableOnTargetLength = true;
            this.onTargetChange = true;
        }
        else{
            this.disableOnTargetLength = false;
            this.onTargetChange = false;
        }
    }

}