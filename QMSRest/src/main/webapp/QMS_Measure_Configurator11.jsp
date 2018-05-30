<!DOCTYPE html>
<!-- saved from url=(0049)https://mfauveau.github.io/angular-query-builder/ -->
<html lang="en" ng-app="app" class="cye-disabled cye-nm ng-scope">
<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<!-- 18/05
<style type="text/css">@charset "UTF-8";[ng\:cloak],[ng-cloak],[data-ng-cloak],[x-ng-cloak],.ng-cloak,.x-ng-cloak,.ng-hide{display:none !important;}ng\:form{display:block;}.ng-animate-block-transitions{transition:0s all!important;-webkit-transition:0s all!important;}.ng-hide-add-active,.ng-hide-remove{display:block!important;}</style>

        
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1">
		-->
		
        <title>QMS Query Builder</title>
		
        <link href="./Query_Builder_files/bootstrap.min.css" rel="stylesheet">
        <link href="./Query_Builder_files/styles.css" rel="stylesheet">
		

<!-- 18/05		
 <style id="nightModeStyle">
html.cye-enabled.cye-nm:not(*:-webkit-full-screen) body,
 html.cye-enabled.cye-nm:not(*:-webkit-full-screen) #cye-workaround-body {-webkit-filter:contrast(91%) brightness(84%) invert(1);}</style><style id="cyebody">html.cye-enabled.cye-lm body{background-color:#cce8cf !important;border-color:rgb(51, 58, 51) !important;background-image:none !important;color:#000000  !important}</style><style id="cyediv">html.cye-enabled.cye-lm div{background-color:#cce8cf !important;border-color:rgb(51, 58, 51) !important;background-image:none !important;color:#000000  !important}</style><style id="cyetable">html.cye-enabled.cye-lm th{background-color:#cce8cf !important;border-color:rgb(51, 58, 51) !important;background-image:none !important;color:#000000  !important}html.cye-enabled.cye-lm td{background-color:#cce8cf !important;border-color:rgb(51, 58, 51) !important;background-image:none !important;color:#000000  !important}</style><style id="cyetextInput">html.cye-enabled.cye-lm input[type=text]{background-color:#cce8cf !important;border-color:rgb(51, 58, 51) !important;background-image:none !important;color:#000000  !important}html.cye-enabled.cye-lm textarea{background-color:#cce8cf !important;border-color:rgb(51, 58, 51) !important;background-image:none !important;color:#000000  !important}</style><style id="cyeselect">html.cye-enabled.cye-lm select{background-color:#cce8cf !important;border-color:rgb(51, 58, 51) !important;background-image:none !important;color:#000000  !important}</style><style id="cyeul">html.cye-enabled.cye-lm ul{background-color:#cce8cf !important;border-color:rgb(51, 58, 51) !important;background-image:none !important;color:#000000  !important}</style><style id="cyeChangeByClick">html.cye-enabled.cye-lm .cye-lm-tag,html.cye-enabled.cye-lm.cye-lm-tag{background-color:#cce8cf !important;border-color:rgb(51, 58, 51) !important;background-image:none !important;color:#000000  !important}
 </style>
 --> 
 </head>
 
  <body  ng-app="QMSHomeManagement" ng-controller="QueryBuilderCtrl">
  <!--
    <a href="https://github.com/mfauveau/angular-query-builder"><img style="position: absolute; top: 0; right: 0; border: 0;" src="./Query_Builder_files/68747470733a2f2f73332e616d617a6f6e6177732e636f6d2f6769746875622f726962626f6e732f666f726b6d655f72696768745f6461726b626c75655f3132313632312e706e67" alt="Fork me on GitHub" data-canonical-src="https://s3.amazonaws.com/github/ribbons/forkme_right_darkblue_121621.png"></a>
	-->
	
    <!--<div class="container ng-scope" ng-app="QMSHomeManagement" ng-controller="QueryBuilderCtrl">-->
	<div class="col-md-12 no-padding-margin main-content" style="height:650px; width:1270px;">
	
            <div class="sub-header">
                <p style="font-size: xx-large;float: left; margin-top: 5.5vh;color: white;margin-left: 3vw" id="heading"><b>Measure Configurator</b></p>
                <div class="button-div">
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
	
	
    <!-- <h1>Angular.js Query Builder</h1> -->
		<br>
		
        <div class="alert alert-info">
            <strong>Example Output</strong><br>
            <span ng-bind-html="output" class="ng-binding">()</span>
        </div> 
	
		 <div class="col-md-12" style="height:230px; width:1270px;">
            <h3>Select Field To be Populated</h3>
             <form class="Field_Selection_Form"> 
			 <table>
			 </table>
               <input type="radio" ng-model="configType" name="Field_Category" 
			   id="Denominator" value="Denominator" ng-change='categoryClick(configType)'>
                <label for="Denominator">Denominator</label>
                 <input type="radio" ng-model="configType" name="Field_Category" id="Denominator_Exclusion" 
				 value="Denominator_Exclusion" ng-change='categoryClick(configType)'>
                <label for="Denominator Exclusion">Denominator Exclusion</label>
 
             <input type="radio" ng-model="configType" name="Field_Category" id="Numerator" 
			 value="Numerator" ng-change='categoryClick(configType)'>
                  <label for="Numerator">Numerator</label>
              <input type="radio" ng-model="configType" name="Field_Category" id="Numerator_Exclusion" 
			  value="Numerator_Exclusion" ng-change='categoryClick(configType)'>
               <label for="Numerator Exclusion">Numerator Exclusion</label>

			   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
					
        <button class="btn btn-cls" ng-click="submit('save')" style="background-color: #A0F3D3;">Save</button>
        <button class="btn btn-cls" ng-click="submit('submit')" style="background-color: #A0F3D3;">Submit</button>
		<button class="btn btn-cls" style="background-color: #A0F3D3;">Clear All</button>
				
                       </form> 
		</div>  
				
	
				   <!-- <br> -->
				   <query-builder group="filter.group" class="ng-isolate-scope">
				   
				   
			   
				   
		<div class="alert alert-warning alert-group ng-scope">
		
		
			<div class="form-inline">
				<!--
				<select ng-options="o.name as o.name for o in operators" ng-model="group.operator" class="form-control input-sm ng-pristine ng-valid"><option value="0" selected="selected">AND</option><option value="1">OR</option>
				</select> 
				
				<button style="margin-left: 5px" ng-click="addCondition()" class="btn btn-sm btn-success"><span class="glyphicon glyphicon-plus-sign"></span> Add Condition</button> -->
			</div>
			<div class="group-conditions">
				<!-- ngRepeat: rule in group.rules | orderBy:'index' -->
			</div>
		
	</query-builder>
    </div> 	 
		
		
	
    </div>

     <script type="text/ng-template" id="/queryBuilderDirective.html"> 
	
    <div class="alert alert-warning alert-group">
        <div class="form-inline">
			<!--
            <select ng-options="o.name as o.name for o in operators" ng-model="group.operator" class="form-control input-sm"></select> 
			-->
		
		
            <button style="margin-left: 5px" ng-click="addCondition()" class="btn btn-sm btn-success">
			<span class="glyphicon glyphicon-plus-sign"></span> Add Condition</button> 
        </div>
        <div class="group-conditions">
            <div ng-repeat="rule in group.rules | orderBy:'index'" class="condition">
                <div ng-switch="rule.hasOwnProperty('group')">
                    <div ng-switch-when="true">
                        <query-builder group="rule.group"></query-builder>
                    </div>
                    <div ng-switch-default="ng-switch-default">
                        <div class="form-inline">
                            <select style="margin-left: 5px" ng-options="c.name as c.name for c in operators" 
							ng-model="rule.operator" class="form-control input-sm">							
							</select> 
							
							<!--
							<select style="margin-left: 5px" ng-options="c.columnName as c.columnName for c in technicalExpressions" 
							ng-model="rule.technicalExpression" class="form-control input-sm">
								<option value="">Please Select Column Name</option>
							</select>							
							-->
							
							<!-- table names -->
							<select style="margin-left: 5px" ng-options="c.name as c.name for c in tableData" 
							ng-model="rule.tableName" ng-change="onChangeTableName(rule.tableName,$index)" class="form-control input-sm">
								<option value="">Please Select Table Name</option>
							</select>
							<!-- column names -->
							<select style="margin-left: 5px" ng-options="c.name as c.name for c in rule.columnData" 
							ng-model="rule.columnName" class="form-control input-sm">
								<option value="">Please Select Column Name</option>
							</select>
							
                            <input style="margin-left: 5px" type="text" ng-model="rule.businessExpression" 
							size=40 class="form-control input-sm" ng-focus="businessExpressionFocus($event)"/>
							
							<!--
                            <input type="text" ng-model="rule.technicalExpression" id="Technical_Expression" 
							size=40 list="technicalExpressionList" class="form-group form-control drop-down drop-margin" 
							ng-focus="rule.technicalExpression=''"/>
							<datalist id="technicalExpressionList" name="technicalExpressionList">
								<option ng-repeat="technicalExp in technicalExpressions track by $index" 
								value="{{technicalExp.columnName}}">
							</datalist>
							-->
							
							<!--
							<input ng-model="rule.technicalExp" list="technicalExpList"  
							class="form-control input-sm" id="technicalExp" type="text"
							ng-focus="rule.technicalExp=''">
							<datalist id="technicalExpList" name="technicalExpList">
								<option ng-repeat="field in fields track by $index" 
								class="form-control input-sm" 
								value="{{field.name}}">
							</datalist>							
							
                            <select ng-options="t.name as t.name for t in fields" ng-model="rule.field" class="form-control input-sm"></select>
							
                            <select style="margin-left: 5px" ng-options="c.name as c.name for c in conditions" ng-model="rule.condition" class="form-control input-sm"></select> 
							
                            <input style="margin-left: 5px" type="text" ng-model="rule.data" class="form-control input-sm"/>
							-->
							<button style="margin-left: 5px" ng-click="addCondition()" class="btn btn-sm btn-success"><span class="glyphicon glyphicon-plus-sign"></span> </button>
							
                            <button style="margin-left: 5px" ng-click="removeCondition($index)" class="btn btn-sm btn-danger"><span class="glyphicon glyphicon-minus-sign"></span></button>							
                        </div>
                    </div>
                </div>
            </div>
			
			<br><br>
			<table>
			<tr><td><b>Remarks: </b></td>
			<td>
				<textarea ng-model="group.remarks" class="form-control input_control form-group " rows="3" cols= "70" 
				type="text" id="Remarks"></textarea>
			</td>		
			
        </div>
    </div>
    </script>

    <!--
	<script src="./Query_Builder_files/angular.min.js"></script>
    <script src="./Query_Builder_files/angular-sanitize.min.js"></script> 
    <script src="./Query_Builder_files/angular-query-builder.js"></script> 
	-->
	
  

</body>
<!-- 1705
<div id="cyeBlackMaskLayer" style="position: fixed; width: 1980px; height: 1080px; z-index: -2147483648; background-color: rgb(19, 19, 19);"></div>
<div id="cye-workaround-body" style="position: absolute; left: 0px; top: 0px; z-index: -2147483646; height: 741px; width: 1600px; background: none 0% 0% / auto repeat scroll padding-box border-box rgb(255, 255, 255);"></div>
<div id="cye-workaround-body-image" style="position: absolute; left: 0px; top: 0px; z-index: -2147483645; height: 741px; width: 1600px; background: none 0% 0% / auto repeat scroll padding-box border-box rgba(0, 0, 0, 0);"></div>
-->
</html>