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
	
     <link rel="stylesheet" href="styles/qms_styles.css">
	 
    <script src="download/angular.min.js"></script>
	<script src="download/angular.js"></script>
	<script src="download/angular-route.js"></script>	
	
	<!--<script src="qms_home.js"></script>	-->
	<script src="qms_common.js"></script>	
	
	<script type="text/javascript">
	var app = angular.module("QMSHomeManagement", []);
	app.controller("PatientProfileController", function($scope, $http, $location, $window, $rootScope) {	
		$scope.patientProfileForm = JSON.parse($window.sessionStorage.getItem("patientProfileData"));
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
        .leftTable
        {
        width: 100%;
        }
         .leftTable tr{
              border-bottom: 2px solid #DDDDDD;

         }
        .VistTable {
            font-family: arial, sans-serif;
            border-collapse: collapse;
            width: 100%;
        }

        .VistTable td,
        th {
            border: 1px solid #dddddd;
            text-align: left;
            padding: 8px;
        }

        .VistTable tr:nth-child(even) {
            background-color: #dddddd;
        }

.numberCircle {
float: right;
    height: 27px;
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
      margin-top: 10px;
    margin-bottom: 10px;
}
#rcorners{
    border-radius: 25px;
    background: #dddddd;
    padding: 20px; 
        width: 150px;
    height: 80px;   
}
.logo{
      width: 15.6vw;
}
        @media screen and (max-width: 767px) {
            .sidenav {
                height: auto;
                padding: 15px;
                width: 100%;
            }
            .row.content {
                height: auto;
            }
        }

        
    </style>
</head>

<body ng-app="QMSHomeManagement" ng-controller="PatientProfileController">
   <header class="header col-md-12" style="width:100%">
        <a href="index.html">
        <img class="logo" src="images/LOGOitcinfotech.jpg"/>
    </a>

        <!-- <div class="reflink">
            <a href="#">Contact Us</a>&nbsp; &nbsp; &nbsp;
            <a href="#">About</a>&nbsp; &nbsp; &nbsp;
            <a href="#">Logout</a>&nbsp; &nbsp; &nbsp;
         <a href="patient_profile.html" id="pp">Patient_Profile</a>
        </div> -->
        <ul class="nav navbar-nav navbar-right" style="padding-right: 14px;display: -webkit-box;float: right;">
<li> <img src="images/speech.png" style="width: auto; height: 5vh;cursor: pointer;margin-top: 0.5vh;float: left;margin-right: 15px;" class=" SpeechIcon"></li>
                <!-- <li><a href="patient_profile.html" id="pp">Patient Profile</a></li> -->
               <li style="margin-right: 20px; margin-top: 5px;"><b>
         <!--<span ng-bind="userName"></span>-->
         <span id="userName" name="userName"></span>
         <br></b></li>
                <li style="margin-right: 25px;margin-top: 0.4vh;">
                    <div>
                        <img src="doc.png" style=" height: 4vh;width: auto;" class="dropdown-toggle" data-toggle="dropdown">
                        <ul class="dropdown-menu" role="menu">
                            <li><a href="#" onclick="logOut()">Logout</a>
                            </li>

                        </ul>
                    </div>
                </li>

                <!--<li><i class="material-icons" style="font-size:25px; font-weight: 600;margin-top: 3px;">info_outline</i></li>-->
            </ul>

    </header>


    <div class="container-fluid" id="container">
        <div class="row content">
            <div class="col-sm-2 sidenav" style="padding-left: 27px;">

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
				<span ng-bind="patientProfileForm.providerFirstName"></span> 
				<span ng-bind="patientProfileForm.providerLastName"></span>
				<br>
				NPI:<span ng-bind="patientProfileForm.providerBillingTaxId"></span></b><br><br>
				Speciality:<span ng-bind="patientProfileForm.providerSpeciality"></span><br>
				Address: <span ng-bind="patientProfileForm.providerAddress1"></span>,<br>
				<span ng-bind="patientProfileForm.providerAddress2"></span></p>

            </div>

            <div class="col-sm-10">
               <div class="container-fluid">
                    <div class="row content">
                        <div class="col-sm-3">
              <p>Name: <b><span ng-bind="patientProfileForm.name"></span></b><br>
			  <!--MRN: <span ng-bind="patientProfileForm.mrn"></span><br>-->
			  BenID: <span ng-bind="patientProfileForm.patId"></span><br>
			  Primary Payer: <span ng-bind="patientProfileForm.primaryPayer"></p>
            </div>
            <div class="col-sm-3">
                <p>Last Date of service: <span ng-bind="patientProfileForm.lastDateService"></span><br>
                Age: <span ng-bind="patientProfileForm.age"></span><br>
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
        <div class="col-sm-12">
          <table class="leftTable">
            <tr>
              <td>
          <div class="div1 Comorbi">
                            
            <span class="headings">Comorbidities</span>
            <span class="numberCircle"><span ng-bind="patientProfileForm.comorbiditiesCount"></span>
			</span>
            <p>
            <span ng-repeat="member in patientProfileForm.comorbidities">{{member}}<br></span><br>
            </p>
           
          </div>
             </td>
             <td>
                <div class="div1">
            <span class="headings">Care Gaps</span>
            <span class="numberCircle"><span ng-bind="patientProfileForm.careGapsCount"></span>
          </div>
          <div class="div1 careGaps">
            <p>
              <!--
              <span ng-bind="patientProfileForm.careGaps1"></span><br>
              <span ng-bind="patientProfileForm.careGaps2"></span><br>
              <span ng-bind="patientProfileForm.careGaps3"></span><br>
              <span ng-bind="patientProfileForm.careGaps4"></span>
              -->
            <span ng-repeat="member in patientProfileForm.careGaps">{{member}}<br></span><br>
            </p>

          </div>
             </td>
             </tr> 

							<!--
							<span ng-bind="patientProfileForm.comorbidity1"></span><br>
							<span ng-bind="patientProfileForm.comorbidity2"></span><br>
							<span ng-bind="patientProfileForm.comorbidity3"></span><br>
							<span ng-bind="patientProfileForm.comorbidity4"></span><br>
							<span ng-bind="patientProfileForm.comorbidity5"></span> 
							-->		
             		<tr>
                 <td>
                   <div class="div1 NextApp">
            <span class="headings">Next Appointment Details</span>
             <!--<span class="numberCircle">4</span>-->
            <p>Next Appointment Date: 15-Aug-2018 <!--<span ng-bind="patientProfileForm.nextAppointmentDate"></span>--><br>
            Physician Name: Test,Physician<!--<span ng-bind="patientProfileForm.physicianName"></span>--><br>
            Department: Test Department<!--<span ng-bind="patientProfileForm.department"></span></p>-->
            
          </div>
                 </td> 
                 <td>
                    <div class="div1 VisitCount">
            <table title="Visit Counts" class="VistTable">
              <tr><th colspan="3" style="text-align: center;">Visit Counts</th></tr>
              <tr>
              <th>IP Visits</th>
              <th>OP Visits</th>
              <th>ER Visits</th>
              </tr>
              <tr>
              <!--
              <td><span ng-bind="patientProfileForm.ipVisitsCount"></span></td>
              <td><span ng-bind="patientProfileForm.opVisitsCount"></span></td>
              <td><span ng-bind="patientProfileForm.erVisitsCount"></span></td> -->
              <td>1</td>
              <td>2</td>
              <td>0</td>                  
              </tr>
            </table>
          </div> 
                 </td>
                </tr>
					<tr style="border:none;">
       <td>
          <div class="div1 Procedures">
            <span class="headings">Procedures</span>
            <p>
            <span ng-bind="patientProfileForm.procedureName1"> - <span ng-bind="patientProfileForm.procedureDateTime1"></span><br>
            <span ng-bind="patientProfileForm.procedureName2"> - <span ng-bind="patientProfileForm.procedureDateTime2"></span>
            </p>
          </div>
      
        </div>
       </td>   
       <td>
          <h2>Medical Prescription</h2>
            <div style="width: 100%;height: 90px;background-color: #dddddd">
              <span ng-bind="patientProfileForm.prescription">
            </div>
       </td>  
          </tr>
        
      <div>
    </div>
  </div>
</div>
</div>
</body>

</html>

