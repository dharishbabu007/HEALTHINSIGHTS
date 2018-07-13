 <!DOCTYPE html>
<html lang="en">

<head>
    <title>Healthcare Insights</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
	<!--
    <link href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css" rel="stylesheet">
	<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
	
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.2.1/jquery.min.js"></script>	
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js"></script>    
    <script src="https://public.tableausoftware.com/javascripts/api/tableau-2.min.js " type="text/javascript"></script> 
    <script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.6.4/angular.min.js"></script>
	<script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.6.4/angular.js"></script>
	<script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.6.4/angular-route.js"></script>
	-->
    <link href="download/bootstrap.min.css" rel="stylesheet">
	<link href="download/Material_Icons.css" rel="stylesheet">
	
    <script src="download/jquery.min.js"></script>	
    <script src="download/bootstrap.min.js"></script>    
    <script src="download/tableau-2.min.js " type="text/javascript"></script> 
    <script src="download/angular.min.js"></script>
	<script src="download/angular.js"></script>
	<script src="download/angular-route.js"></script>		
	
    <script src="scripts/annyang.min.js"></script> <!-- Voice Recognition -->
    <script src="scripts/responsivevoice.js"></script> <!-- Voice Response -->	
	
	<script src="./Query_Builder_files/angular-sanitize.min.js"></script> 
	<script src="qms_home.js"></script>
	<script src="qms_common.js"></script> 
	
	<!--
	<script src="./Query_Builder_files/angular-query-builder.js"></script> 
	-->
	
	
<link rel="stylesheet" href="styles/qms_styles.css">

 
</head>
<style type="text/css">


@media (min-width: 992px){
    .left-pane{
    width: 12.666667%;
}
.main-pane{
    width: 87.333333%;
}
}
 </style>   

<body ng-app="QMSHomeManagement" ng-controller="HLeftController">
<!--
 <nav class="navbar navbar-inverse">
    <div class="container-fluid">
        <div class="navbar-header">
            <button type="button" class="navbar-toggle" data-toggle="collapse" data-target="#myNavbar">
                  <span class="icon-bar"></span>
                    <span class="icon-bar"></span>
                   <span class="icon-bar"></span>                        
                  </button>
                     
            <a href="index.html"> <img class="logo" src="images/LOGOitcinfotech.jpg"/></a>
       
        </div>
        <div class="collapse navbar-collapse" id="myNavbar">
            <ul class="nav navbar-nav">
                 <li style="margin-left: 50px;"><a onClick="Home(); return false;">Home</a></li> 
            </ul>
            <ul class="nav navbar-nav navbar-right speechicon" style="    display: -webkit-box;    float: right;">
         <img src="images/speech.png"  style="float: right;margin-top: 15px;width: auto; height:6vh; cursor:pointer;" onclick="">
                 <li><a href="patient_profile.html" id="pp">Patient Profile</a></li> 
               <li style="margin-right: 20px; margin-top: 5px;">
			   <b><span ng-bind="userName"></span></b><br>
			   </li>
                <li style="margin-right: 25px;margin-top: 5px;">
                    <div>
                        <img src="images/doc.png" width="40px" height="40px" class="dropdown-toggle" data-toggle="dropdown">
                        <ul class="dropdown-menu" role="menu">
                            <li><a href="#" ng-click="logOut()">Logout</a>
                            </li>

                        </ul> 
                    </div>
                </li>

                <li><i class="material-icons" style="font-size:25px; font-weight: 600;margin-top: 3px;">info_outline</i></li>
            </ul>
        </div>
    </div>
    </nav>-->
	 <header class="header col-md-12">
        <a href="index.html">
        <img class="logo" src="images/LOGOitcinfotech.jpg"/>
    </a>

        <!-- <div class="reflink">
            <a href="#">Contact Us</a>&nbsp; &nbsp; &nbsp;
            <a href="#">About</a>&nbsp; &nbsp; &nbsp;
            <a href="#">Logout</a>&nbsp; &nbsp; &nbsp;
         <a href="patient_profile.html" id="pp">Patient_Profile</a>
        </div> -->
        <ul class="nav navbar-nav navbar-right" style="padding-right: 14px;display: -webkit-box;
    float: right;">
  <li> <img src="images/speech.png" style="width: auto; height: 6vh;cursor: pointer;margin-top: 10px;float: left;margin-right: 15px;" class=" SpeechIcon"></li>
                <!-- <li><a href="patient_profile.html" id="pp">Patient Profile</a></li> -->
               <li style="margin-right: 20px; margin-top: 5px;"><b><span ng-bind="userName"></span><br></b></li>
                <li style="margin-right: 25px;margin-top: 5px;">
                    <div>
                        <img src="images/doc.png" width="40px" height="40px" class="dropdown-toggle" data-toggle="dropdown">
                        <ul class="dropdown-menu" role="menu">
                            <li><a href="#" style="color: black;
    background-color: #FFFFFF;
    border-radius: 5px;
    width: 100px;" ng-click="logOut()">Logout</a>
                            </li>

                        </ul>
                    </div>
                </li>

                <!--<li><i class="material-icons" style="font-size:25px; font-weight: 600;margin-top: 3px;">info_outline</i></li>-->
            </ul>

    </header>
    <div class="col-md-12 no-padding-margin" id="main_field">
            <div class="col-md-2 no-padding-margin nav-border-box left-pane">
                <p class="quicklink"><b>Quick Links</b></p>

                <p ng-class="{active:activeTab == '#!'}" ng-click="isActive('#!')"><a href="#/!">QMS Home</a></p>
                <p ng-class="{active:activeTab == '#!Measure_Library'}" ng-click="isActive('#!Measure_Library')"><a href="#!Measure_Library">Measure Library</a></p>
                <p ng-class="{active:activeTab == '#!Final_Creator'}" ng-click="isActive('#!Final_Creator')"><a href="#!Final_Creator"  ng-click="goToMeasureCreator()">Measure Creator</a></p>
                <!-- <p><a href="Measure_Editor.html">Measure Editor</a></p> -->
                <p ng-class="{active:activeTab == '#!Measure_Worklist'}" ng-click="isActive('#!Measure_Worklist')"><a href="#!Measure_Worklist">Measure Worklist</a></p>
                <!-- <p><a href="#">Measure Configurator</a></p> -->
            </div>
            <div class="col-md-10 no-padding-margin main-content main-pane">
                   <div class="col-md-12 no-padding-margin" ng-view >

                   </div>
                <div class="col-md-12 footer no-padding-margin">
                   

            </div>
            </div>
     
    </div>


    </body>
    </html>
	<script type="text/javascript">
    if (annyang) {
      $('.SpeechIcon').click(function() {
      var clicks = $(this).data('clicks');
       if (clicks) {
            annyang.start();
            $(this).attr('src',"speech.png");
            console.log("started");
            } 
            else {
               
            annyang.abort();
             $(this).attr('src',"speech_mute.png");
            console.log("stopped");

            }
            $(this).data("clicks", !clicks);
           });
    }
</script>

