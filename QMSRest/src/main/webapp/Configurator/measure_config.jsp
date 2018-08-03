<!DOCTYPE html>

<html lang="en" ng-app="app" class="cye-disabled cye-nm ng-scope">
<head>
   <title>Measure Configurator</title>
   <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
 <link href="download/bootstrap.min.css" rel="stylesheet">	
<script src="download/jquery.min.js"></script>	
<script src="download/bootstrap.min.js"></script>    
<script src="download/angular.min.js"></script>
<script src="download/angular.js"></script>
<script src="download/angular-route.js"></script>		
<script src="Query_Builder_files/angular-sanitize.min.js"></script> 
<script src="Query_Builder_files/angular-query-builder.js"></script> 	
<link rel="stylesheet" href="styles/qms_styles.css">
 </head>
 
  <body  ng-app="QMSHomeManagement" ng-controller="QueryBuilderCtrl">
 
	
		<div class="col-md-12 no-padding-margin ng-scope" style="background-color: #e4e5e6;">
	
            <div class="col-md-12" style="background-color: #e4e5e6;">
			
                <span style="color: #23282C;font-size: x-large;" id="heading"><b>Measure Configurator</b> 
				<b><span ng-bind="selectedMeasureTitle" style="font-size: 20px"></span></b></span>
            </div>		
	
	
 
		<br>
		<div class="col-md-12">
        <div class="alert alert-info" style="background-color:white; border-color: white;">
            <strong>Business Expression</strong><br>
            <span ng-bind-html="output" class="ng-binding">()</span>
        </div> 
	

	<div class="col-md-12" style="background-color:white;height:500px;width: 100%;border-radius: 5px;"> 
	
      
			<label for="Select_Field">Select Field To be Populated</label>
             <form class="Field_Selection_Form"> 
			 <table>
			 </table>
			 
             <input type="radio" ng-model="configType" name="Field_Category" id="Numerator" 
			 value="Numerator" ng-change='categoryClick(configType)'>
                  <label for="Numerator">Numerator</label>			 			 
				&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
               <input type="radio" ng-model="configType" name="Field_Category" 
			   id="Denominator" value="Denominator" ng-change='categoryClick(configType)'>
                <label for="Denominator">Denominator</label>
				&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
              <input type="radio" ng-model="configType" name="Field_Category" id="Numerator_Exclusion" 
			  value="Numerator_Exclusion" ng-change='categoryClick(configType)'>
               <label for="Numerator Exclusion">Numerator Exclusion</label>				
			   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                 <input type="radio" ng-model="configType" name="Field_Category" id="Denominator_Exclusion" 
				 value="Denominator_Exclusion" ng-change='categoryClick(configType)'>
                <label for="Denominator Exclusion">Denominator Exclusion</label>

				
			   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			
			
        <button class="btn btn-cls btn-primary" ng-click="submit('save')">Save</button>
        <button class="btn btn-cls btn-primary" ng-click="submit('submit')">Submit</button>
		<button class="btn btn-cls btn-primary" >Clear All</button> 
				
                       </form> 
	
					   
    <query-builder group="filter.group" class="ng-isolate-scope">
    <div class="alert alert-warning alert-group ng-scope" >
        <div class="form-inline">
	
        </div>
        <div class="group-conditions">
        
        </div>
    </div>
    </query-builder>					   
					   
     </div> 	  
		
		

	
    </div>
	
		
     <script type="text/ng-template" id="/queryBuilderDirective.html"> 
		
	<!--<div class="alert alert-warning alert-group" style="background-color:lightyellow;overflow-y: scroll;height:280px; width:1200px;">-->
    <div class="alert alert-warning alert-group" style="background-color:white;overflow-y: scroll;height:280px;width: 100%; border-color: #9E9E9E;">
		<!--
        <div class="form-inline">
            <select ng-options="o.name as o.name for o in operators" ng-model="group.operator" class="form-control input-sm"></select> 
			-->
            <button style="margin-left: 5px" ng-click="addCondition()" class="btn btn-sm btn-success">
			<span class="glyphicon glyphicon-plus-sign"></span> Add Condition</button> 
			
        <!-- </div> -->
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
							<select style="margin-left: 5px;width: 250px;" ng-options="c.name as c.name for c in tableData" 
							ng-model="rule.tableName" ng-change="onChangeTableName(rule.tableName,$index)" class="form-control input-sm">
								<option value="">Please Select Table Name</option>
							</select>
							<!-- column names -->
							<select style="margin-left: 5px;width: 250px;" ng-options="c.name as c.name for c in rule.columnData" 
							ng-model="rule.columnName" class="form-control input-sm">
								<option value="">Please Select Column Name</option>
							</select>
							
                            <input style="margin-left: 5px;width: 250px;" type="text" ng-model="rule.businessExpression" 
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
			<table cellspacing="10" cellpadding="10">
			<tr>
				<td><b>Remarks: </b></td>
				<td>
					<textarea ng-model="group.remarks" class="form-control input_control form-group " rows="3" cols= "50" 
					type="text" id="Remarks"></textarea>
				</td>		
				<!--
			<td>
			   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			   
				<button class="btn btn-cls bnt-style" ng-click="submit('save')">Save</button>
				<button class="btn btn-cls" ng-click="submit('submit')">Submit</button>
				<button class="btn btn-cls">Clear All</button>			
			</td> -->
			<tr>
			</table>
			
        </div>
    </div>
    </script>
</div>
  
  

</body>

</html>