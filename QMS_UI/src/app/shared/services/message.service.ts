import { Injectable } from '@angular/core';
import { Message } from 'primeng/primeng';

@Injectable()
export class MessageService {
    message: Message[];

    constructor() {
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
