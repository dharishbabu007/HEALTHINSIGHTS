<!DOCTYPE html>
<html lang="en">

<head>
     <title>Measure Creator</title>  
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
   /* background: #C8C8C8;*/
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

</style>

<body ng-app="QMSHomeManagement" ng-controller="MeasureCreateController">
   
   	
        <div class="col-md-12 no-padding-margin main-content">
            <div class="sub-header">
                <p style="font-size: xx-large;float: left; margin-top: 5.5vh;color: white;margin-left: 3vw" id="heading"><b>Measure Creator</b></p>
                <i class="material-icons" data-toggle="tooltip" data-placement="right" title="Measure Creator info" style="font-size: 25px;font-weight: 600;margin-top: 6vh;float: left;margin-left: 1vw;cursor: pointer;">info_outline</i>

                <div class="button-div">
                    <div class="col-md-12 submit-cls">
                        <button ng-click="submitAdd('save')" class="btn btn-cls" style="background-color: #EFEFEF">Save</button> 
                        <button ng-click="submitAdd('submit')" class="btn btn-cls" style="background-color: #EFEFEF">Submit</button>  
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
            <div class=" col-md-12 no-padding-margin" style="background-color: #fff;height: 60vh; overflow: auto;height: inherit;    border-left: #060606 1px solid;">
      <form action="#" class="form-horizontal ws-validate">
	  
    <div class="col-md-6" style="padding-top: 3vh;padding-left: 85px;"> <label for="Program_Name">Program Name*</label>
       <input ng-model="measureForm.programName" list="measureProgramName" 
	   class="form-group form-control drop-down drop-margin" id="Program_Name" 	ng-change="onChangeProgramName()"   
	   required>
	    <datalist id="measureProgramName" name="measureProgramName">
            <option ng-repeat="measureProgramName in measureProgramNames track by $index" value="{{measureProgramName.name}}">
        </datalist>		
        <label for="Measure_Title">Measure Title*</label>
        <input ng-model="measureForm.name" class="form-group form-control drop-down drop-margin" id="Measure_Title" required>
        <label for="Description">Description*</label>
        <input ng-model="measureForm.description" class="form-group form-control drop-down drop-margin" id="Description" required>
        <label for="Target_Age">Target Age*</label>
        <input ng-model="measureForm.targetAge" class="form-group form-control drop-down drop-margin" id="Target_Age" required>
        
         <label for="Measure_Domain">Measure Domain</label>
        <input ng-model="measureForm.measureDomain" class="form-group form-control drop-down drop-margin" 
		id="Measure_Domain" list="measureDomain">
        <datalist id="measureDomain" name="measureDomain">
            <option ng-repeat="measureDomain in measureDomains track by $index" value="{{measureDomain.name}}">
        </datalist>
      
        <label for="Measure_Category">Measure Category</label>
        <input ng-model="measureForm.measureCategory" list="measureCategory" 
		class="form-group form-control drop-down drop-margin" id="Measure_Category" type="text">
        <datalist id="measureCategory" name="measureCategory">
            <option ng-repeat="measureCategory in measureCategories track by $index" value="{{measureCategory.name}}">
        </datalist>		
       
        <label for="Type">Type</label>
        <input  list="measureType"  id="Type" class="form-group form-control drop-down" >
        <datalist ng-model="measureForm.type" id="measureType" name="measureType">
            <option ng-repeat="measureType in measureTypes track by $index" value="{{measureType.name}}"></option>
        </datalist>			
   


         <label for="Clinical_Conditions">Clinical Conditions</label>
        <input ng-model="measureForm.clinocalCondition" class="form-group form-control drop-down drop-margin" id="Clinical_Conditions">
    
    </div>                               
    <div class="col-md-6" style="padding-top:3vh;padding-left: 85px">
      
         <label for="Denominator">Denominator*</label>
        <textarea ng-model="measureForm.denominator" class="form-control input_control form-group " rows="3" id="Denominator" type="text" required></textarea>
         <label for="Denominator_Exclusions">Denominator Exclusions</label>
        <textarea ng-model="measureForm.denomExclusions" class="form-control input_control form-group " rows="3" id="Denominator_Exclusions" type="text"></textarea>
           <label for="Numerator">Numerator*</label>
        <textarea ng-model="measureForm.numerator" class="form-control input_control form-group " rows="3" type="text" id="Numerator" required></textarea>
        <label for="Numerator_Exclusions">Numerator Exclusions</label>
        <textarea ng-model="measureForm.numeratorExclusions" class="form-control input_control form-group " rows="3"  type="text" id="Numerator_Exclusions"></textarea>
        <label for="Target">Target</label>
        <input ng-model="measureForm.target" class="form-group form-control drop-down drop-margin" id="Target" >
       
    </div>
    
            </div>
            </form>

        <!--     <div class="col-md-1"></div> -->
            </div>
          
        </div>
    </div>

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