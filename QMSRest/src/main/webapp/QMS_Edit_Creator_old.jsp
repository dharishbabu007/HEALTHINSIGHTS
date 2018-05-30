<!DOCTYPE html>
<html lang="en">

<head>
    <title>Bootstrap Example</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.2.1/jquery.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js"></script>
    <link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
    <!--  stylesheet for help icon -->
    <link rel="stylesheet" href="qms_styles.css">
	
	<script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.4.4/angular.js"></script>  
	<script type="text/javascript">
	var app = angular.module("QMSHomeManagement", []);
	//Worklist
	app.controller("MeasureEditController", function($scope, $http, $location) {					
				
		var selectedMeasureId = $location.search().measureId;
		var action = $location.search().action;
				
		$scope.measureForm = {					 
			id : -1,
			name : "",
			programName : "",
			type : "",
			steward : "",
			clinocalCondition : "",
			status : "",
			reviewComments : "",
			reviewedBy : "",
			targetAge : "",
			measureDomain : "",
			measureCategory : "",
			target : "",
			description : "",
			denominator : "",
			denomExclusions : "",
			numerator : "",
			numeratorExclusions : ""				 
		};	

		_getMeasureLib();	
		
		function _getMeasureLib() {
			$http({
				method : 'GET',
				url : 'http://localhost:8081/curis/qms/measure_list/'+selectedMeasureId
			}).then(function successCallback(response) {
				$scope.measureForm = response.data;
				console.log(" measure --> " + $scope.measureForm);
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}   
				
		//enable the input fields
		$scope.isDisabled = true;
		$scope.editClick = function() {
			$scope.isDisabled = false;
		}
		
		$scope.submitEdit = function () {			
			$http.put('http://localhost:8081/curis/qms/work_list/'+selectedMeasureId, 
			$scope.measureForm)		
			.then(function successCallback(response) {
				//$scope.measureForm = response.data;				
				console.log(response.data);
				_clearFormData();
				alert("Update measure creator success. ");
			}, function errorCallback(response) {
				console.log(response.statusText);
			});
		}	

		function _clearFormData() {
			$scope.measureForm.id = -1;
			$scope.measureForm.name = "";
			$scope.measureForm.programName = "";
			$scope.measureForm.type = "";
			$scope.measureForm.steward = "";
			$scope.measureForm.clinocalCondition = "";
			$scope.measureForm.status = "";
			$scope.measureForm.reviewComments = "";
			$scope.measureForm.reviewedBy = "";
			$scope.measureForm.targetAge = "";
			$scope.measureForm.measureDomain = "";
			$scope.measureForm.measureCategory = "";
			$scope.measureForm.target = "";
			$scope.measureForm.description = "";
			$scope.measureForm.denominator = "";
			$scope.measureForm.denomExclusions = "";
			$scope.measureForm.numerator = "";
			$scope.measureForm.numeratorExclusions = "";				 			
		}		
    });	
	</script>	
	
</head>
<style type="text/css">
.body{
  background-color: #dddddd;
}
.Creator{
  top: 0; bottom: 0; left: 0; right: 0;
  border-collapse: separate;
            border-spacing: 15px;
            font-family: sans-serif;
            background-color: #fff;
            margin: 20px;
            width: 97.5%;
            height: 350px;
}
.Creator>tbody>tr>td{
  border-top: none;
}
.select-box {
     position:relative;
     background-color:white;
     border:solid grey 0px;
     width:250px;
     height:30px;
     background-color: #E0F5ED;
 }
 .select-box select {
     position:absolute;
     top:0px;
     left:0px;
     font-size:15px;
     border:none;
     width:245px;
     margin:0;
         height: 25px;
         background-color: #E0F5ED;
 }
 .select-box input {
     position:absolute;
     top:0px;
     left:0px;
     width:200px;
     padding:6px;
     font-size:15px;
     border:none;
     background-color: #E0F5ED;
 }
 .select-box select:focus, .select-box input:focus {
     outline:none; 
 }
 .input_control{
  width: 350px;
  margin-bottom: 29px;
    height: 35px;
    border: 0px solid #777474;
    border-radius: 0px;
    background-color: #E0F5ED;
    border: 1px solid #ccc;
    border-radius: 4px;
 }
textarea {

   resize: none;
   
}
::-webkit-input-placeholder {
   font-weight: bold;
   color: #449073;
}
.drop-down{
    background-color: #E0F5ED;
    width: 350px;
}
.drop-margin{
    margin-bottom: 30px;
}
.submit-cls{
    text-align: center;
    padding: 10px 0px;
    background: #C8C8C8;
}
.btn-cls{
    height: 30px;
    font-weight: bold;
    
}
.btn-cls:hover{
    
    border:1px solid black;
}
</style>

<body ng-app="QMSHomeManagement" ng-controller="MeasureEditController">
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
                   <li style="margin-right: 20px; margin-top: 5px;"><b>Admin,Admin<br></b></li>
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
    <div class="col-md-12 no-padding-margin">
        <div class="col-md-2 no-padding-margin nav-border-box">
            <p class="quicklink"><a href="#"><b>Quick Links</b></a></p>
            <p><a href="qms_landing.jsp">QMS Home</a></p>
            <p><a href="QMS_measure_list.jsp">Measure Library</a></p>
            <p class="active-tab"><a href="QMS_Create_Creator.jsp">Measure Creator</a></p>
           <!--  <p><a href="Measure_Editor.html">Measure Editor</a></p> -->
            <p><a href="QMS_Measure_Worklist.jsp">Measure Worklist</a></p>
            <!-- <p><a href="#">Measure Configurator</a></p> -->
        </div>
        <div class="col-md-10 no-padding-margin main-content">
            <div class="sub-header">
                <p style="font-size: xx-large;float: left; margin-top: 5.5vh;color: white;margin-left: 3vw" id="heading"><b>Measure Editor</b></p>
                <div class="button-div">
                    <form class="search-form" class="form-inline" role="form" method="post" action="//www.google.com/search" target="_blank">
                        <div class="input-group">
                            <input type="text" class="form-control search-form" placeholder="Search Measure">
                            <span class="input-group-btn"><button type="submit" class="btn btn-primary search-btn" data-target="#search-form" name="q" style="margin-top: 9px;padding: 0px;"><img src="SearchIcon.png" height= "33.5px">
                        </button></span>
                        </div>
                    </form>
                </div>
            </div>
            <div class="sub-content" id="sub-content">
            <!-- <div class="col-md-1"></div> -->
            <div class=" col-md-12 no-padding-margin" style="background-color: #fff;height: 60vh;">
               
    <div class="col-md-6" style="padding-top: 3vh;"><center>
	<form>
        <input ng-model="measureForm.programName" ng-disabled="isDisabled" name="browser" class="form-group form-control drop-down drop-margin" placeholder="Program Name">
		<!--
        <input list="browsers" ng-model="measureForm.programName" name="browser" class="form-group form-control drop-down drop-margin" placeholder="Program Name">
        <datalist id="browsers">
            <option value="Internet Explorer">
            <option value="Firefox">
            <option value="Chrome">
            <option value="Opera">
            <option value="Safari">
        </datalist> -->

        <input ng-model="measureForm.targetAge" ng-disabled="isDisabled" name="browser" class="form-group form-control drop-down drop-margin" placeholder="Target Age">

        <input ng-model="measureForm.measureDomain" ng-disabled="isDisabled" name="browser" class="form-group form-control drop-down drop-margin" placeholder="Measure Domian" >

        <input ng-model="measureForm.measureCategory" ng-disabled="isDisabled" name="browser" class="form-group form-control drop-down drop-margin" placeholder="Measure Category" >

        <input ng-model="measureForm.type" ng-disabled="isDisabled" name="browser" class="form-group form-control drop-down drop-margin" placeholder="Type" >

        <input ng-model="measureForm.clinocalCondition" ng-disabled="isDisabled" name="browser" class="form-group form-control drop-down drop-margin" placeholder="Clinical Condition">

        <input ng-model="measureForm.target" ng-disabled="isDisabled" name="browser" class="form-group form-control drop-down drop-margin" placeholder="Target">
    </center>
    </div>
    <div class="col-md-6" style="padding-top:3vh">
        <center>
        <input ng-model="measureForm.name" ng-disabled="isDisabled" 
		name="browser" class="form-group form-control drop-down drop-margin" placeholder="Measure Title">
        <input ng-model="measureForm.description" ng-disabled="isDisabled" 
		name="browser" class="form-group form-control drop-down drop-margin" placeholder="Description">
        <textarea ng-model="measureForm.denominator" ng-disabled="isDisabled" 
		class="form-control input_control form-group " rows="2" id="Denominator" type="text" 
		placeholder="Denominator"></textarea>
        <textarea ng-model="measureForm.denomExclusions" ng-disabled="isDisabled" 
		class="form-control input_control form-group " rows="2" id="Denominator" type="text" 
		placeholder="Denominator Exclusions"></textarea>
        <textarea ng-model="measureForm.numerator" ng-disabled="isDisabled" 
		class="form-control input_control form-group " rows="2" id="Denominator" type="text" 
		placeholder="Numerator"></textarea>
        <textarea ng-model="measureForm.numeratorExclusions" ng-disabled="isDisabled" 
		class="form-control input_control form-group " rows="2" id="Denominator" type="text" 
		placeholder="Numerator Exclusions"></textarea>

        </center>
    </div>
    
    
            </div>
            <div class="col-md-12 submit-cls">
		<button ng-click="editClick()" class="btn btn-cls" style="background-color: #EFEFEF">Edit</button>	        

		<button ng-click="submitEdit()" class="btn btn-cls" style="background-color: #EFEFEF">Save</button>
				
				<!-- 
		<input type="submit" class="btn btn-cls" style="background-color: #EFEFEF" value="Save" >
				-->
        <button class="btn btn-cls" style="background-color: #EFEFEF">Submit</button>
    </div>
	

        <!--     <div class="col-md-1"></div> -->
            </div>
            <div class="footer">

            </div>
        </div>
    </div>
    </div>
	</form>
</body>

</html>


<script>
  $(document).ready(function () {
    $('tr').click(function () {
      var selected = $(this).hasClass("highlight");
      /*var selected = $(this).hasClass("highlight");*/
    $('tr').removeClass("highlight");
    if(!selected)
            $(this).addClass("highlight");
       
        /*if(this.style.background == "none" || this.style.background =="") {

             
            $(this).css('background', '#8ae8c4');
        }
        else {
          $(this).css('background', "");
          }*/

    });

});
</script>