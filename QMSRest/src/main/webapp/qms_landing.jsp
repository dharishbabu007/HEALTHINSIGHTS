<!DOCTYPE html>
<html lang="en">

<head>
 


     <script src="scripts/annyang.min.js"></script> <!-- Voice Recognition -->
    <script src="scripts/responsivevoice.js"></script> <!-- Voice Response -->
    
    <script src="https://public.tableausoftware.com/javascripts/api/tableau-2.min.js " type="text/javascript"></script> 

	<script src="scripts/Search.js"></script>	
	
	<!--
	<script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.6.4/angular.js"></script>
	<script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.6.4/angular-route.js"></script>	  
	
	<script src="qms_home.js"></script>		-->
	
</head>

<body ng-app="QMSHomeManagement" ng-controller="QMSHomeController">
    

    <!-- <div>
        <div class="col-md-12 col-sm-12 no-padding-margin header">
            <img class="logo" src="Curis_Landing.jpg" /></a>
                <ul class="nav navbar-nav navbar-right">
                   <li><a href="patient_profile.html" id="pp">Patient Profile</a></li>
                    <li><b>Dr.Ram Kumar<br>Cardiac Surgen</b></li>
                    <li>
                     <div>
                       <img src="doc.png" width="40vw" height="40vh" class="dropdown-toggle" data-toggle="dropdown">
                                <ul class="dropdown-menu" role="menu">
                                   <li><a href="#" >Logout</a>
                                    </li>
        
                                </ul>
                       </div>
                    </li>
                     <li><i class="material-icons">info_outline</i></li>
                </ul>
        </div>
         -->
        
            
            <div class="col-md-12 no-padding-margin main-content">
                <div class="col-md-12 sub-header">
                    <p style="font-size: xx-large;float: left; margin-top: 5.5vh;color: white;margin-left: 3vw" id="heading"><b>QMS Home</b></p>
                    <div class="button-div">
                    <form class="search-form" class="form-inline" role="form" method="post" action="/" target="_blank" >
                        <div class="input-group">
                            <input type="text" class="form-control search-form" placeholder="Search" id="search">
                            <span class="input-group-btn"><button type="submit" class="btn btn-primary search-btn" data-target="#search-form" name="q" style="margin-top: 5px;padding: 0px;"><img src="images/SearchIcon.png" height= "33.5px">
                        </button></span>
                        </div>
                    </form>
                </div>
                </div>
                <div class="col-md-12 sub-content">
                    <div class="col-md-1">

                    </div>
                    <div class="col-md-12 content-table-div">
                    <table class="sub-content-table"> <tr>
                    <td>
                    <div class="col-md-3 div-content">
                        <div class="div-content-box">
                            <div class="div_sub-content">
                                    <img src="images/Reembursement.png" width="60px" height="60px" style="float: left; margin-top: 5px;">
                                    <p> <br>Quality Programs</p><br>
                                </div>
                            <div class="border-box">
                                
                                <div class="tags">
<!-- 
                                    <ul id="list">
                                     <li><a href="#">MIPS</a><img src="images/ListView_icon.png" class="icon-class pull-right" >
                                        <a href="MIPS.html"><img src="images/Dashboard_icon.png" class="icon-class pull-right" style="margin-right: 3px;"></a> <br><br></li>

                                    <li><a href="#">this is a sample sentence to check<br><br></a></li>
                                    <li><a href="#">ACO<br><br></a></li>
                                    <li id="mips-list">
                                        <a href="#">MIPS sample sentence to check</a>
                                        <img src="images/ListView_icon.png" class="icon-class pull-right" >
                                        <a href="MIPS.html"><img src="images/Dashboard_icon.png" class="icon-class pull-right" style="margin-right: 3px;"></a>
                                        <br><br>
                                    </li>
                                    
                                    <li><a href="#">ACO</a><br><br></li>
                                    <li><a href="#">MIPS</a><br><br></li>
                                    <li><a href="#">ACO</a><br><br></li>
                                   
                                    </ul>
                                     -->
									 
									 <!--
                                     <table class="inside-content">
                                         <tr>
                                             <td class="td-content">
                                                 This is a sample sentence to check whatever sooo omnnudiergf
                                             </td>
                                             <td class="td-images">
                                                <img src="images/ListView_icon.png" class="pull-right" >
                                        <a href="MIPS.html"><img src="images/Dashboard_icon.png" class=" pull-right" style="margin-right: 3px;"></a>
                                                 
                                             </td>
                                         </tr>
                                         <tr>
                                             <td>
                                                 This is a sample sentence to check whatever sooo omnnudiergf
                                             </td>
                                             <td>
                                                <img src="images/ListView_icon.png" class="pull-right" >
                                        <a href="MIPS.html"><img src="images/Dashboard_icon.png" class=" pull-right" style="margin-right: 3px;"></a> 
                                             </td>
                                         </tr>
                                         <tr>
                                             <td>
                                                 This is a sample sentence to check to check
                                             </td>
                                             <td>
                                                <img src="images/ListView_icon.png" class="pull-right" >
                                        <a href="MIPS.html"><img src="images/Dashboard_icon.png" class=" pull-right" style="margin-right: 3px;"></a> 
                                             </td>
                                         </tr>
                                     </table> -->
                                    <table class="inside-content" id="tablesearch">
									<tr ng-repeat="program in reimbursementPrograms track by $index" ng-if="
                                      program == 'Merit-Based Incentive Payment System (MIPS) Program'">
									 
									<td class="td-content"> 
									{{program}}
									</td>
									
									<td class="td-images">
									
									<img src="images/ListView_icon.png" class="pull-right" 
									onMouseOver="this.style.cursor='pointer'" 
									ng-click="qmsHomeIconClick('Reimbursement', program)">

									<a 
									ng-href="MIPS.html">
									<img src="images/Dashboard_icon.png" class=" pull-right" style="margin-right: 3px;">
									</a>
									</td>
									</tr>
									
									<tr ng-repeat="program in reimbursementPrograms track by $index" ng-if="
                                      program == 'CMS Star'">
									 
									<td class="td-content">
									{{program}}
									</td>
									
									<td class="td-images">
									<img src="images/ListView_icon.png" class="pull-right" 
									onMouseOver="this.style.cursor='pointer'" 
									ng-click="qmsHomeIconClick('Reimbursement', program)">
									<a 
									ng-href="CMS_Stars.html">
									<img src="images/Dashboard_icon.png" class=" pull-right" style="margin-right: 3px;">
									</a>
									</td>
									
									</tr>	
                                   <tr ng-repeat="program in reimbursementPrograms track by $index" ng-if="
                                      program == 'The Healthcare Effectiveness Data and Information Set (HEDIS)'">
									 
									<td class="td-content">
									{{program}}
									</td>
									
									<td class="td-images">
									<img src="images/ListView_icon.png" class="pull-right" 
									onMouseOver="this.style.cursor='pointer'" 
									ng-click="qmsHomeIconClick('Reimbursement', program)">
									<a 
									ng-href="Hedis.html">
									<img src="images/Dashboard_icon.png" class=" pull-right" style="margin-right: 3px;">
									</a>
									</td>
									
									</tr>										
									
									
									<tr ng-repeat="program in reimbursementPrograms track by $index" ng-if="
                                      program != 'Merit-Based Incentive Payment System (MIPS) Program' && program !='CMS Star'&& program !='The Healthcare Effectiveness Data and Information Set (HEDIS)'">
									 
									<td class="td-content">		
									{{program}}
									</td>
									
									<td class="td-images">
									<img src="images/ListView_icon.png" style="float:right;" 
									onMouseOver="this.style.cursor='pointer'" 
									ng-click="qmsHomeIconClick('Reimbursement', program)">
									</td>
									</tr>
									
									</table> 
									 

                                </div>
                            </div>
                        </div>
                    </div>
                    </td>  
                    <td>
                    <div class="col-md-3 div-content">
                        <div class="div-content-box">
                            <div class="div_sub-content">
                                    <img src="images/Clinical.png" width="60px" height="60px" style="float: left; margin-top: 5px;">
                                    <p> <br>Clinical Conditions</p><br>
                                     </div>
                            <div class="border-box">
                                
                               
                                <div class="tags">
                                    <table class="inside-content" id="tablesearch">
										<!--
                                        <tr>
                                            <td class="td-content">
											<a href="#" id="myBtn">Diabetes</a>
                                            </td>
                                            <td>
                                                
                                            </td>
                                        </tr> -->
										
									<tr ng-repeat="condition in clinicalConditions track by $index">
									
									<td class="td-content">		
									{{condition}}
									</td>
									
									<td class="td-images">
									<img src="images/ListView_icon.png" style="float:right;" 
									onMouseOver="this.style.cursor='pointer'" 
									ng-click="qmsHomeIconClick('Clinical', condition)">									
									</td>
									</tr>										
										
                                    </table>									
                                     <!-- <li><br><br></li>
                                    <li><a href="#">COPD</a><br><br></li>
                                    <li><a href="#">Asthma</a><br><br></li>
                                    <li><a href="#">CHF</a><br><br></li>
                                    <li><a href="#">Diabetes</a><br><br></li>
                                    <li><a href="#">COPD</a><br><br></li>
                                    
                                   
                                    </ul> -->
                                    
                                </div>
                            </div>
                        </div>
                    </div>
                    </td>
                    <td>
                    <div class="col-md-3 div-content">
                        <div class="div-content-box">
                               <div class="div_sub-content">
                                    <img src="images/NQF.png" width="60px" height="60px" style="float: left; margin-top: 5px;">
                                    <p> <br>NQF Domain</p><br>
                                     </div>
                            <div class="border-box">
                             
                               
                                <div class="tags">
                                   <!--  <ul>
                                      <li><a href="#">Care Coordination</a><br><br></li>
                                    <li><a href="#">Patient Safety</a><br><br></li>
                                    <li><a href="#">Care Coordination</a><br><br></li>
                                    <li><a href="#">Patient Safety</a><br><br></li>
                                    <li><a href="#">Care Coordination</a><br><br></li>
                                    <li><a href="#">Patient Safety</a><br><br></li>
                                    
                                    </ul> -->
                                    <table class="inside-content" id="tablesearch">
									
                                    <!--    <tr>
                                            <td class="td-content">
                                            Diabetes
                                            </td>
                                            <td>
                                                
                                            </td>
                                        </tr> -->
										
									<tr ng-repeat="domain in nqfDomains track by $index">
									<td class="td-content">		
									{{domain}}
									</td>
									
									<td class="td-images">
									<img src="images/ListView_icon.png" style="float:right;" 
									onMouseOver="this.style.cursor='pointer'" 
									ng-click="qmsHomeIconClick('NQF', domain)">																		
									</td>
									
									</tr>
										
                                    </table>
                                    
                                </div>
                            </div>
                        </div>
                    </div>
                    </td>
                </tr>
                     </table>
                     </div>
                </div>
            
            </div>
        </div>
    

   
</body>

</html>



<script>
if (annyang) {
  // These are the voice commands in quotes followed by the function
  var commands = {
    //commands for Facility_Name
    
    'Show Health Insights HomePage': function() {  responsiveVoice.speak('Showing Health Insights HomePage', "UK English Female"); selectCurisHome();},
  'Show QMS HomePage': function() {  responsiveVoice.speak('Showing QMS HomePage', "UK English Female"); selectQmsHome();},
  'Show Diabetes Eye Exam': function() {  responsiveVoice.speak('Showing Diabetes Eye Exam', "UK English Female"); selectDiabetesExam();},
     //Command To reset all the filters
    'start over': function() { startover(); responsiveVoice.speak('starting over'); }

 };
  
  // Add commands to annyang
  annyang.addCommands(commands);
  
  // Start listening.
  annyang.start({continuous:false});
  annyang.debug();
//  annyang.start({ autoRestart: true });
}
</script>
 
<script>

function selectCurisHome()
  {
    window.location.href="index.html";
}
function selectQmsHome()
  {
    window.location.href="qms_landing.html";
}
function selectDiabetesExam()
  {
    window.location.href="DiabetesEyeExam.html";
}
/*function selectNext()
    {
    activeSheet2.getWorksheets()[2].selectMarksAsync('Next', 'Next', 'REPLACE'); 
    }   

*/
    
//Start Viz Over
function startover()
    {
       
    viz.revertAllAsync();
    }

</script>
