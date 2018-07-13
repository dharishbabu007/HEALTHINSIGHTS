<!DOCTYPE html>
<html lang="en">

<head>
    <title>Measure Library</title>  
<script src="Sorting.js"></script>
<script src="scripts/Search.js"></script>			

	
</head>
<style type="text/css">

.library{
    background-color: #fff;
    margin:20px;
    width: 97.5%;
 
}
.library tbody{
    /* height: 100px;
    overflow: auto;*/
    display:block;
    max-height: 56vh;
overflow-y:scroll;
}
    .library thead,tbody,tr {
        /*background-color: #37a279;
        color: #fff;*/
        display:table;
width:100%;
table-layout:fixed;
   
    }
.library thead {
        background-color: #007D72;
      
   
    }
    .library tr {
        color:#01776C;
        cursor: pointer;
    }
.library thead>tr>th{
  color: #fff;
      font-size: 17px;
}
    .highlight {
        background-color:#a9dad9;
        background-color:#a9dad9;
    }
    
</style>

<body  ng-app="QMSHomeManagement" ng-controller="MeasureListController">
  

        <div class="col-md-12 no-padding-margin main-content">
            <div class="sub-header">
                <p style="font-size: xx-large;float: left; margin-top: 5.5vh;color: white;margin-left: 3vw" id="heading"><b>Measure Library</b></p>
                <div class="button-div">
                    <form class="search-form" class="form-inline" role="form" method="post" action="//www.google.com/search" target="_blank">
                        <div class="input-group">
                            <input type="text" class="form-control search-form" placeholder="Search" id="search">
                            <span class="input-group-btn"><button type="submit" class="btn btn-primary search-btn" data-target="#search-form" name="q" style="margin-top: 5px;padding: 0px;"><img src="SearchIcon.png" height= "33.5px">
                        </button></span>
                        </div>
                    </form>

					<!--
                    <input type="button" name="" value="Copy" class="btn-edit btn btn-primary btn-mini">
					-->
					<button ng-click="copyClick('copy')" class="btn-edit btn btn-primary btn-mini">Copy</button>
					
                   <!-- <a ng-click="viewEditClick()">  
				   <input type="button" name="View_Edit" value="View / Edit" ng-click="viewEditClick()" class="btn-edit btn btn-primary btn-mini">
				   <!-- </a> -->
				   <button ng-click="viewEditClick('view')" class="btn-edit btn btn-primary btn-mini">View / Edit</button>
				   
				   <!--
                    <input type="button" name="" value="Dashboard" class="btn-edit btn btn-primary btn-mini"
					onClick="window.location.href='DiabetesEyeExam.html'"> -->
					<button ng-click="dashboardClick()" class="btn-edit btn btn-primary btn-mini">Dashboard</button>
					
                </div>
            </div>
            <div class="sub-content" id="sub-content">
                <table class="table library" id="tablesearch">
                    <thead>
                        <tr>
                    <th class="sortable">Measure ID</th>
                    <th class="sortable">Measure Name</th>
                    <th>Program Name</th>
                    <th>Measure Type</th>
                    <th>Measure Steward</th>
                    <th>Clinical Condition</th>
                    </tr>
                          </thead>
                    <tbody>
					
						   <tr ng-repeat="measure in measureList" ng-click="viewMeasureLib(measure);setSelected()" class="{{selected}}">
							<td>{{measure.id}}</font></td>
							<td>{{measure.name}}</font></td>
							<td>{{measure.programName}}</td>
							<td>{{measure.type}}</td>
							<td>{{measure.steward}}</td>
							<td>{{measure.clinocalCondition}}</td>        
						   </tr>					
					<!--
                        <tr>
                            <td>12345</td>
                            <td>common disease Antiplatelet Therapy</td>
                            <td>MIPS</td>
                            <td>pass</td>
                            <td>No Comments</td>
                            <td>Dr.BRam</td>

                        </tr>
                        <tr>
                            <td>1234985</td>
                            <td>common disease Antiplatelet Therapy</td>
                            <td>MIPS</td>
                            <td>pass</td>
                            <td>No Comments</td>
                            <td>Dr.BRam</td>
                        </tr>   -->
                        
                        
                    </tbody>

                </table>
            </div>
          
        </div>
    </div>

</body>

</html>
<script>
  $(document).ready(function () {
    $('tbody tr').click(function () {
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