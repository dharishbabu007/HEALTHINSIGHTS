<!DOCTYPE html>
<html lang="en">

<head>
    <title>Bootstrap Example</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
	<!--
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.2.1/jquery.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
    <link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
	-->
		
    <link href="download/bootstrap.min.css" rel="stylesheet">
	<link href="download/Material_Icons.css" rel="stylesheet">
	
    <script src="download/jquery.min.js"></script>	
    <script src="download/bootstrap.min.js"></script>    
    <script src="download/tableau-2.min.js " type="text/javascript"></script> 	
	
    <script src="annyang.min.js"></script> <!-- Voice Recognition -->
    <script src="responsivevoice.js"></script> <!-- Voice Response -->
    
     <link rel="stylesheet" href="qms_styles.css">
	 
    <script src="download/angular.min.js"></script>
	<script src="download/angular.js"></script>
	<script src="download/angular-route.js"></script>	
	
	<script type="text/javascript">
	var app = angular.module("QMSHomeManagement", []);
	app.controller("PatientProfileController", function($scope, $http, $location) {	
				
		var selectedMemberId = $location.search().mrnId;
		$scope.memberId = selectedMemberId;		
		$scope.patientProfileForm = { 
			 patId : "",
			 ptyId : "",
			 emrPatId : "",
			 ssn : "",
			 mrn : "",
			 firstName : "",
			 middleName : "",
			 lastName : "",
			 city : "",
			 addLine1 : "",
			 addLine2 : "",
			 state : "",
			 county : "",
			 country : "",
			 race : "",
			 zip : "",
			 deathDate : "",
			 birthDate : "",
			 emailAddress : "",
			 maritialStatus : "",
			 language : "",
			 gender : "",
			 lngtd : "",
			 lattd : "",	
			 ethniCity : "",
			 currFlag : "",
			 createDate : "",
			 updateDate : "",
			
			//for other fields - AggregateFactMember
			 comorbidity1 : "",
			 comorbidity2 : "",
			 comorbidity3 : "",
			 comorbidity4 : "",
			 comorbidity5 : "",
			 comorbidity6 : "",
			 comorbidity7 : "",
			 comorbidity8 : "",
			 comorbidity9 : "",
			 comorbidity10 : "",
			 careGaps1 : "",
			 careGaps2 : "",
			 careGaps3 : "",
			 careGaps4 : "",
			 ipVisitsCount : "",
			 opVisitsCount : "",
			 erVisitsCount : "",
			 prescription : "",
			 nextAppointmentDate : "",
			 physicianName : "",
			 department : "",
			 procedureName1 : "",
			 procedureDateTime1 : "",
			 procedureName2 : "",
			 procedureDateTime2 : "",			 
			 lastDateService : "",	
			 
			 //for provider details
			 providerFirstName : "",
			 providerLastName : "",
			 providerAddress1 : "",
			 providerAddress2 : "",
			 providerBillingTaxId : "",
			 providerSpeciality : "",

			 //new fileds added
			 address : "",
			 name : "",
			 phone : "",
			 age : "",
			 primaryPayer : "",
			 mraScore : "",
			 risk : ""
		};	

		_getPatientProfile();	
		
		function _getPatientProfile() {
			$http({
				method : 'GET',
				url : 'http://localhost:8082/curis/qms/spv/'+memberId
			}).then(function successCallback(response) {
				$scope.patientProfileForm = response.data;
				console.log(" patient Profile Data --> " + $scope.patientProfileForm);
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}   
    });	
	</script>
	
	
    <style>
    .icons{
          
    margin-top: 20px;
    float: right;
    }
    #container{
      /*margin-top: 8vh;*/
    }
   
p{
  margin-top: 2vh;
}
p:hover{
    background-color: #fff;
}

        .sidenav {
            background-color: #f1f1f1;
            height: 91.8vh;
        }
     .aside {
            height: 20vh;
        }
       .main-name{
        
       }
        .naming1{
          float: left;
        }
        .naming{
           margin-left: 2px;
        }

        hr {
          margin: 0px;
            border-color: #dddddd;
            border-style: solid;
            border-width: 1px;
            
        }
        #left-col{
         
            border-style: solid;
            border-color: black;
            border-width: 1px;
        }

        table {
            font-family: arial, sans-serif;
            border-collapse: collapse;
            width: 100%;
        }

        td,
        th {
            border: 1px solid #dddddd;
            text-align: left;
            padding: 8px;
        }

        tr:nth-child(even) {
            background-color: #dddddd;
        }

.numberCircle {
float: right;
    border-radius: 50%;
    width: 25px;
    background: #09F17D;
    text-align: center;
    font: 20px Arial, sans-serif;
}
.headings{
  
    font-size: 1.5em;
    font-weight: bold;
}
.div1{
  width: 60%;
}
#rcorners{
    border-radius: 25px;
    background: #dddddd;
    padding: 20px; 
        width: 150px;
    height: 80px;   
}
        @media screen and (max-width: 767px) {
            .sidenav {
                height: auto;
                padding: 15px;
            }
            .row.content {
                height: auto;
            }
        }
    </style>
</head>

<body ng-app="QMSHomeManagement" ng-controller="PatientProfileController">
 <!--  <header class="header col-md-12" >
        <a href="index.html">
          <img class="logo" src="Curis_Logo.jpg"/>
  </a>

        <div class="reflink">
            <a href="#">Contact Us</a>&nbsp; &nbsp; &nbsp;
            <a href="#">About</a>&nbsp; &nbsp; &nbsp;
            <a href="#">Logout</a>
        </div>

    </header> -->
    <nav class="navbar navbar-inverse">
    <div class="container-fluid">
        <div class="navbar-header">
            <button type="button" class="navbar-toggle" data-toggle="collapse" data-target="#myNavbar">
                  <span class="icon-bar"></span>
                    <span class="icon-bar"></span>
                   <span class="icon-bar"></span>                        
                  </button>
                 
            <a href="index.html"> <img class="logo" src="LOGOitcinfotech.jpg"/></a>
       
        </div>
        <div class="collapse navbar-collapse" id="myNavbar">
            <ul class="nav navbar-nav">
                <!-- <li style="margin-left: 50px;"><a onClick="Home(); return false;">Home</a></li> -->
            </ul>
            <ul class="nav navbar-nav navbar-right">

                <!-- <li><a href="patient_profile.html" id="pp">Patient Profile</a></li> -->
                <li style="margin-right: 20px; margin-top: 5px;"><b>Admin<br></b></li>
                <li style="margin-right: 25px;margin-top: 5px;">
                    <div>
                        <img src="doc.png" width="40px" height="40px" class="dropdown-toggle" data-toggle="dropdown">
                        <ul class="dropdown-menu" role="menu">
                            <li><a href="#">Logout</a>
                            </li>

                        </ul>
                    </div>
                </li>

                <li><i class="material-icons" style="font-size:25px; font-weight: 600;margin-top: 3px;">info_outline</i></li>
            </ul>
        </div>
    </div>
    </nav>

    <div class="container-fluid" id="container">
        <div class="row content">
            <div class="col-sm-3 sidenav" style="padding-left: 27px;">

                <img src="patient_pic.jpg" width="150px" height="150px">
                <!--<p>Address:<br>11,HIL Road,Atlanta<br>Georgia<br>Phone:111-111-2134<br>Email:roh.lind@yahoo.com</p> -->
				<p>Address:<br>
				<span ng-bind="patientProfileForm.address"></span><br>
				Phone:<span ng-bind="patientProfileForm.phone"></span><br>
				<!--
				<span ng-bind="patientProfileForm.addLine1"></span><br>
				<span ng-bind="patientProfileForm.addLine2"></span><br>
				<span ng-bind="patientProfileForm.city"></span><br>
				-->
				
				Email:<span ng-bind="patientProfileForm.emailAddress"></span></p>
                <br><hr id="left-col"><br>
                <!-- <img id="doc" src="doc_pic.png" width="150px" height="150px"> -->
                <p>PCP Name:<br><b>
				<span ng-bind="patientProfileForm.name"></span> 
				<!--
				<span ng-bind="patientProfileForm.providerFirstName"></span> 
				<span ng-bind="patientProfileForm.providerLastName"></span>
				-->
				<br>
				NPI:<span ng-bind="patientProfileForm.providerBillingTaxId"></span></b><br><br>
				Speciality:<span ng-bind="patientProfileForm.providerSpeciality"></span><br>
				Address: <span ng-bind="patientProfileForm.providerAddress1"></span>,<br>
				<span ng-bind="patientProfileForm.providerAddress2"></span></p>

            </div>

            <div class="col-sm-9">
               <div class="container-fluid">
                    <div class="row content">
                        <div class="col-sm-3">
              <p>Name: <b><span ng-bind="patientProfileForm.firstName"></span></b><br>
			  MRN: <br>
			  BenID: <span ng-bind="memberId"></span><br>
			  Primary Payer: <span ng-bind="patientProfileForm.primaryPayer"></p>
            </div>
            <div class="col-sm-3">
                <p>Last Date of service: <span ng-bind="patientProfileForm.lastDateService"><br>
                Age: <span ng-bind="patientProfileForm.age"><br>
                Gender: <span ng-bind="patientProfileForm.gender"></span><br>
                Race/Ethinticity: <span ng-bind="patientProfileForm.ethniCity"></span><br></p>
                  </div>
                
               <div class="col-sm-3 MRA">
                <br><br><br>
             <p id="rcorners">MRA Score: <i class="numberCircle"><span ng-bind="patientProfileForm.mraScore"></i><br>
			 Risk: <span ng-bind="patientProfileForm.risk"></p>
             </div>
               <div class="col-sm-3">
                  <!-- <i class="fa fa-remove" style="font-size:35px;float: right;"></i> -->
                  <div class="icons">
                  <img src="email.png" width="50px" height="30px">

                  <img src="phone.jpg" width="30px" height="30px">
                  </div>
                </div>
              </div>
            </div>
                
             <br><hr><br>
          <div class="container-fluid">
                    <div class="row content">
                        <div class="col-sm-6">
                           
                           <div class="div1">
                            
                             <span class="headings">Comorbidities</span>
                             <span class="numberCircle">4</span>
                    
                           </div>
                            <p>
							<span ng-bind="patientProfileForm.comorbidity1"></span><br>
							<span ng-bind="patientProfileForm.comorbidity2"></span><br>
							<span ng-bind="patientProfileForm.comorbidity3"></span><br>
							<span ng-bind="patientProfileForm.comorbidity4"></span><br>
							<span ng-bind="patientProfileForm.comorbidity5"></span>
							</p>
                            <br><hr><br>
                            <div class="div1">
                             <span class="headings">Next Appointment Details</span>
                             <span class="numberCircle">4</span>
                           </div>
                            <p>Next Appointment Date: <span ng-bind="patientProfileForm.nextAppointmentDate"><br>
							Physician Name: <span ng-bind="patientProfileForm.physicianName"><br>
							Department: <span ng-bind="patientProfileForm.department"></p>
                           <br><hr><br>
                            
                            
                            <span class="headings">Procedures</span>
                            
                             <p>
							 <span ng-bind="patientProfileForm.procedureName1"> - <span ng-bind="patientProfileForm.procedureDateTime1"><br>
							 <span ng-bind="patientProfileForm.procedureName2"> - <span ng-bind="patientProfileForm.procedureDateTime2">
							 </p>
                      
                           </div>
                        <div class="col-sm-6">
                           
                         <div class="div1">
                            
                             <span class="headings">Care Gaps</span>
                             <span class="numberCircle">3</span>
                    
                           </div>
                            <p>
							<span ng-bind="patientProfileForm.careGaps1"></span><br>
							<span ng-bind="patientProfileForm.careGaps2"></span><br>
							<span ng-bind="patientProfileForm.careGaps3"></span><br>
							<span ng-bind="patientProfileForm.careGaps4"></span></p>
                            <br><hr><br>
                            <table title="Visit Counts">
                              <tr><th colspan="3" style="text-align: center;">Visit Counts</th></tr>
                                <tr>
                                    <th>IP Visits</th>
                                    <th>OP Visits</th>
                                    <th>ER Visits</th>
                                </tr>
                                <tr>
                                    <td><span ng-bind="patientProfileForm.ipVisitsCount"></td>
                                    <td><span ng-bind="patientProfileForm.opVisitsCount"></td>
                                    <td><span ng-bind="patientProfileForm.erVisitsCount"></td>
                                </tr>


                            </table>
                            
                             <br><br><hr><br>
                             <h2>Medical Prescription</h2>
                             <div style="width: 100%;height: 90px;background-color: #dddddd">
							 <span ng-bind="patientProfileForm.prescription">
							 </div>
                        </div>
                    <div>
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
 
  'Open Control Tower': function() {  responsiveVoice.speak('Showing Patient Profile', "UK English Female"); selectMIPS();},
  'Open Health Insights HomePage': function() {  responsiveVoice.speak('Showing Health Insights HomePage', "UK English Female"); selectCurisHome();},
  'Open QMS HomePage': function() {  responsiveVoice.speak('Showing QMS HomePage', "UK English Female"); selectQmsHome();},
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



function selectMIPS()
  {
    window.location.href="MIPS.html";
}
function selectCurisHome()
  {
    window.location.href="index.html";
}
function selectQmsHome()
  {
    window.location.href="qms_landing.jsp";
}
  
//Start Viz Over
function startover()
  {
       
  viz.revertAllAsync();
  }

</script>
