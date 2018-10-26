
import {Injectable} from '@angular/core'
declare let annyang:any
@Injectable()
export class AnnyangService {
  start() {

    annyang.start({continuous:false});
  }

  commands = {};
 debug(){
  annyang.debug(true);
 }
 addcommands(commands){
 annyang.addCommands(commands);
 }
    
}