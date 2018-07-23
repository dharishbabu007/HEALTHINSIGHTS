<!DOCTYPE html>
<html lang="en">

<head>
   <title>Measure Editor</title>
	<!-- commented on 18/05
	<script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.6.4/angular.min.js"></script>
	<script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.6.4/angular.js"></script>
	
	<script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.6.4/angular-route.js"></script>
   	<script src="qms_home.js"></script>
	-->
    <script src="scripts/base64.js"></script>
	 <link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
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

.drop-down{
    background-color: #E0F5ED;
    width: 350px;
}
.drop-margin{
    margin-bottom: 25px;
}
.submit-cls{
    text-align: center;
    padding: 10px 0px;
 
        margin-bottom: 0px


}
 .form-horizontal .form-group {
     margin-right: 0px; 
     margin-left: 0px; 

}

.btn-cls{
    height: 30px;
    font-weight: bold;
    
}
.btn-cls:hover{
    
    border:1px solid black;
}
.form-control[disabled], fieldset[disabled] .form-control {
cursor: default;
}


</style>

<body ng-app="QMSHomeManagement" ng-controller="MeasureEditController">
    
   
        <div class="col-md-12 no-padding-margin main-content">
            <div class="sub-header">
                <p style="font-size: xx-large;float: left; margin-top: 5.5vh;color: white;margin-left: 3vw" id="heading"><b>Measure Editor</b></p>
                 <i class="material-icons" data-toggle="tooltip" data-placement="right" title="Measure Editor info" style="font-size: 25px;font-weight: 600;margin-top: 6vh;float: left;margin-left: 1vw;cursor: pointer;">info_outline</i>
                <div class="button-div">
                      <div class="col-md-12 submit-cls">
        
        <button ng-click="editClick()" class="btn btn-cls" style="background-color: #EFEFEF">Edit</button>          
        <button ng-click="submitEdit('save')" class="btn btn-cls" style="background-color: #EFEFEF">Save</button>
                <!-- 
        <input type="submit" class="btn btn-cls" style="background-color: #EFEFEF" value="Save" >
                -->
        <button ng-click="submitEdit('submit')" class="btn btn-cls" style="background-color: #EFEFEF">Submit</button> 

        <button class="btn-cls btn" style="background-color: #EFEFEF;"><a href="javascript://Save as TXT" id="submitLink" style="color: black;background-color: #EFEFEF;">Download</a></p> </button>      
            
    </div>
					<!--
                    <form class="search-form" class="form-inline" role="form" method="post" action="//www.google.com/search" target="_blank">
                        <div class="input-group">
                            <input type="text" class="form-control search-form" placeholder="Search">
                            <span class="input-group-btn"><button type="submit" class="btn btn-primary search-btn" data-target="#search-form" name="q" style="margin-top: 5px;padding: 0px;"><img src="images/SearchIcon.png" height= "33.5px">
                        </button></span>
                        </div>
                    </form> -->
                </div>
            </div>
            <div class="sub-content" id="sub-content">
            <!-- <div class="col-md-1"></div> -->
            <div class=" col-md-12 no-padding-margin" style="background-color: #fff;height: inherit; overflow: auto;border-left:rgba(6, 6, 6, 0.12) 1px solid;">
 <form action="#" class="form-horizontal ws-validate" method="post" id="formToSave">
    <div class="col-md-6" style="padding-top: 3vh;padding-left: 85px;"> <label for="Program_Name">Program Name*</label>
       <input ng-model="measureForm.programName" list="measureProgramName" ng-disabled="isDisabled"  
	   class="form-group form-control drop-down drop-margin" id="Program_Name" 	ng-change="onChangeProgramName()"   
	   required>
	    <datalist id="measureProgramName" name="measureProgramName">
            <option ng-repeat="measureProgramName in measureProgramNames track by $index" value="{{measureProgramName.name}}">
        </datalist>			   
        
        <label for="Measure_Title">Measure Title*</label>
        <input ng-model="measureForm.name" ng-disabled="isDisabled" name="" class="form-group form-control drop-down drop-margin" id="Measure_Title" required>
        <label for="Description">Description*</label>
        <input ng-model="measureForm.description" ng-disabled="isDisabled" name="" class="form-group form-control drop-down drop-margin" id="Description" required>
        <label for="Target_Age">Target Age*</label>
        <input ng-model="measureForm.targetAge" ng-disabled="isDisabled" class="form-group form-control drop-down drop-margin" id="Target_Age" required>
        
         <label for="Measure_Domain">Measure Domain</label>
        <input ng-model="measureForm.measureDomain" ng-disabled="isDisabled" class="form-group form-control drop-down drop-margin" id="Measure_Domain" 
		list="measureDomain">
        <datalist id="measureDomain" name="measureDomain">
            <option ng-repeat="measureDomain in measureDomains track by $index" value="{{measureDomain.name}}">
        </datalist>		
        
        <label for="Measure_Category">Measure Category</label>
        <input ng-model="measureForm.measureCategory" list="measureCategory" ng-disabled="isDisabled" 
		class="form-group form-control drop-down drop-margin" id="Measure_Category" type="text">
        <datalist id="measureCategory" name="measureCategory">
            <option ng-repeat="measureCategory in measureCategories track by $index" value="{{measureCategory}}">
        </datalist>				
        
       <!--  <label for="Type">Type</label>
        <input ng-model="measureForm.type" ng-disabled="isDisabled" list="measureType" class="form-group form-control drop-down drop-margin" id="Type"  >
        <datalist id="measureType" name="measureType">
            <option ng-repeat="measureType in measureTypes track by $index" value="{{measureType.name}}">
        </datalist>	 -->
         <label for="Type">Type</label>
        <select class="form-group form-control drop-down drop-margin" ng-model="measureForm.type" ng-disabled="isDisabled" id="measureType" name="measureType" ng-options="m.name for m in measureTypes">   
        </select>					
       
         <label for="Clinical_Conditions">Clinical Conditions</label>
        <input ng-model="measureForm.clinocalCondition" ng-disabled="isDisabled" class="form-group form-control drop-down drop-margin" id="Clinical_Conditions">
       

        
  
    </div>
    <div class="col-md-6" style="padding-top:3vh;padding-left: 85px">
      
         <label for="Denominator">Denominator*</label>
        <textarea ng-model="measureForm.denominator" ng-disabled="isDisabled" class="form-control input_control form-group " rows="3" id="Denominator" type="text" required></textarea>
         <label for="Denominator_Exclusions">Denominator Exclusions</label>
        <textarea ng-model="measureForm.denomExclusions" ng-disabled="isDisabled" class="form-control input_control form-group " rows="3" id="Denominator_Exclusions" type="text"></textarea>
           <label for="Numerator">Numerator*</label>
        <textarea ng-model="measureForm.numerator" ng-disabled="isDisabled" class="form-control input_control form-group " rows="3" type="text" id="Numerator" required></textarea>
        <label for="Numerator_Exclusions">Numerator Exclusions</label>
        <textarea ng-model="measureForm.numeratorExclusions" ng-disabled="isDisabled" class="form-control input_control form-group " rows="3"  type="text" id="Numerator_Exclusions"></textarea>
        <label for="Target">Target</label>
        <input ng-model="measureForm.target" ng-disabled="isDisabled" name="" class="form-group form-control drop-down drop-margin" id="Target" >
       
    </div>
    
            </div>
          

        <!--     <div class="col-md-1"></div> -->
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



function buildData(){
    console.log($("#Program_Name").val());
    console.log($("#Measure_Title").val());
    var txtData = "Program Name        : "+$("#Program_Name").val()+
              "\r\nMeasure Title       : "+$("#Measure_Title").val()+
              "\r\nDescription         : "+$("#Description").val()+
              "\r\nTarget Age          : "+$("#Target_Age").val()+
              "\r\nMeasure Domain      : "+$("#Measure_Domain").val()+
              "\r\nMeasure Category    : "+$("#Measure_Category").val()+
              "\r\nMeasure Type        : "+$("#Measure_Type").val()+
              "\r\nClinical Conditions : "+$("#Clinical_Conditions").val()+
              "\r\nDenominator         : "+$("#Denominator").val()+
              "\r\nDenominator_Exclusion   : "+$("#Denominator_Exclusions").val()+
              "\r\nNumerator           : "+$("#Numerator").val()+
              "\r\nNumerator_Exclusion   : "+$("#Numerator_Exclusions").val()+
              "\r\nTarget              : "+$("#Target").val()

            ;

    return txtData;
}
// This will be executed when the document is ready
$(function(){
    // This will act when the submit BUTTON is clicked
    $("#formToSave").submit(function(event){
        event.preventDefault();
        var txtData = buildData();
        console.log("came here1");
        window.location.href="data:application/octet-stream;base64,"+Base64.encode(txtData);
    });

    // This will act when the submit LINK is clicked
    $("#submitLink").click(function(event){
        var txtData = buildData();
         console.log("came here2");
        $(this).attr('download','EditorData.txt')
            .attr('href',"data:application/octet-stream;base64,"+Base64.encode(txtData));
             console.log("came here3");
    });
});
</script>