
import {Injectable} from '@angular/core'
declare let annyang:any
@Injectable()
export class AnnyangService {
  start() {
    annyang.addCommands(this.commands);
    
    annyang.start({continuous:false});
  }

  commands = {};
 debug(){
  annyang.debug(true);
 }
}