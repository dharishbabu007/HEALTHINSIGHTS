import { Injectable } from '@angular/core';
import { Message } from 'primeng/primeng';
import {ConfirmationService} from 'primeng/api';


@Injectable()
export class MessageService {
    message: Message[];

    constructor(private ConfirmationService:ConfirmationService) {
        this.message = [];
    }

    success(detail: string, summary?: string, multiple: boolean = true): void {
        this.pushMsg('success', detail, summary, multiple);
    }

    info(detail: string, summary?: string, multiple: boolean = true): void {
        this.pushMsg('info', detail, summary, multiple);
    }

    warning(detail: string, summary?: string, multiple: boolean = true): void {
        this.pushMsg('warn', detail, summary, multiple);
    }

    error(detail: string, summary?: string, multiple: boolean = true): void {
        this.pushMsg('error', detail, summary, multiple);
    }
     

    confirm(detail: string, summary?: string, multiple: boolean = true): void {
        this.ConfirmationService.confirm({
            message: 'Are you sure that you want to proceed?',
            header: 'Confirmation',
            icon: 'pi pi-exclamation-triangle',
            accept: () => {
                this.pushMsg('success', detail, summary, multiple);
            },
            reject: () => {
                this.pushMsg('success', detail, summary, multiple);
            }
        });
    } 

    
    pushMsg(severity, detail, summary, multiple) {
        if (!multiple) {
            this.message[0] = {
                severity: severity, summary: summary, detail: detail
            };
        } else {
            this.message.push({
                severity: severity, summary: summary, detail: detail
            });
        }
    }

    clear() {
        this.message = [];
    }
    

}
