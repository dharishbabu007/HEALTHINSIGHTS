<!DOCTYPE html>
<html lang="en">

<head>
   
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
    overflow-y: scroll;*/
    display:block;
    max-height: 440px;
overflow-y:scroll;
}
.library thead, tbody tr {
display:table;
width:100%;
table-layout:fixed;
}
    .library thead {
        background-color: #007D72;
        color: #fff;
   
    }

    .library tr {
       color:#01776C;;
        cursor: pointer;
    }
.library thead>tr>th{
  color: #fff;
  font-size: 17px;
}
.library td{
  max-width: 20%;
}
    .highlight {
        background-color: #a9dad9;
    }
    .btn-config{
      font-weight: bold;
      width: 150px;
    }
</style>

<body ng-app="QMSHomeManagement" ng-controller="WorklistController">
   
   
        <div class="col-md-12 no-padding-margin main-content">
            <div class="sub-header">
                <p style="font-size: xx-large;float: left; margin-top: 5.5vh;color: white;margin-left: 3vw" id="heading"><b>Measure Worklist</b></p>
                <div class="button-div">
                    <form class="search-form" class="form-inline" role="form" method="post" action="//www.google.com/search" target="_blank" style="float:right">
                        <div class="input-group">
                            <input type="text" class="form-control search-form" placeholder="Search">
                            <span class="input-group-btn"><button type="submit" class="btn btn-primary search-btn" data-target="#search-form" name="q" style="margin-top: 5px;padding: 0px;"><img src="SearchIcon.png" height= "33.5px">
                        </button></span>
                        </div>
                    </form>
					<div> 
					<!--
					<a href="QMS_Measure_Configurator.html">
					<input type="button" name="" value="Measure Configurator" class="btn-config btn-edit btn btn-primary btn-mini">
					</a> 
					-->
					<!--
                    <input type="button" name="" value="Reject" class="btn-config btn-edit btn btn-primary btn-mini">					                    
                    <input type="button" name="" value="Approve" class="btn-config btn-edit btn btn-primary btn-mini">					
                    <input type="button" name="" value="View" class="btn-config btn-edit btn btn-primary btn-mini">					
					-->
					<button ng-click="measureConfigClick()" class="btn-edit btn btn-primary btn-mini">Measure Configurator</button>
					<button ng-click="statusClick('Re-work')" class="btn-edit btn btn-primary btn-mini">Reject</button>
					<button ng-click="statusClick('Approved')" class="btn-edit btn btn-primary btn-mini">Approve</button>
					<button ng-click="viewClick('view')" class="btn-edit btn btn-primary btn-mini">View</button>	
                   </div>					
                </div>
            </div>
            <div class="sub-content" id="sub-content">
                <table class="table library">

                    <thead>
                      <tr>
                        <th class="sortable">Measure ID</th>
                        <th>Measure Name</th>
						<th>Program Name</th>
						<th class="sortable">Status</th>
						<th>Review Comments</th>
						<th>Reviewed By</th>
                      </tr>
                    </thead>
                    
                    <tbody style="overflow-y: scroll;">
					
                        <tr ng-repeat="work in workList" ng-click="setSelected(work)" class="{{selected}}">
                            <td>{{work.id}}</td>
                            <td>{{work.name}}</td>
                            <td>{{work.programName}}</td>
                            <td>{{work.status}}</td>
                            <td>{{work.reviewComments}}</td>
                            <td>{{work.reviewedBy}}</td>
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
                            <td>12345</td>
                            <td>common disease Antiplatelet Therapy</td>
                            <td>MIPS</td>
                            <td>pass</td>
                            <td>No Comments</td>
                            <td>Dr.BRam</td>
                        </tr> -->
                       
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