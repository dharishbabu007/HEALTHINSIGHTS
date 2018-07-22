<!DOCTYPE html>
<html lang="en">

<head>
   <title>Measure Worklist</title>
<script src="Sorting.js"></script>

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
    max-height: 56vh;
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
	
.rejectColor {
    color: red;
}

.approveColor {
    color: green;
}

.reviewColor {
    color: #FF9800;
}

.inProgressColor {
    color: blue;
}
</style>

<body ng-app="QMSHomeManagement" ng-controller="WorklistController">
   
   
        <div class="col-md-12 no-padding-margin main-content">
            <div class="sub-header">
                <p style="font-size: xx-large;float: left; margin-top: 5.5vh;color: white;margin-left: 3vw" id="heading"><b>Measure Worklist</b></p>
                 <i class="material-icons" data-toggle="tooltip" data-placement="right" title="Measure worklist info" style="font-size: 25px;font-weight: 600;margin-top: 6vh;float: left;margin-left: 1vw;cursor: pointer;">info_outline</i>
                <div class="button-div">
                    <form class="search-form" class="form-inline" role="form" style="float:right">
                        <div class="input-group">
                            <input type="text" class="form-control search-form" placeholder="Search Worklist" ng-model="searchWork">
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
					
					<button ng-click="measureConfigClick()" ng-disabled="isDisabled" class="btn-edit btn btn-primary btn-mini">Measure Configurator</button>
					<button ng-click="statusClick('Re-work')" class="btn-edit btn btn-primary btn-mini">Reject</button>
					<button ng-click="statusClick('Approved')" class="btn-edit btn btn-primary btn-mini">Approve</button>					
					<button ng-click="viewClick('view')" class="btn-edit btn btn-primary btn-mini">View</button>	
					-->
                   </div>					
                </div>
            </div>
            <div class="sub-content" id="sub-content">
                <table class="table library">

                    <thead>
                      <tr>
                        <th style="width: 10%" class="sortable">Measure ID</th>
                        <th style="width: 20%">Measure Name</th>
						<th style="width: 20%">Program Name</th>
						<th class="sortable">Status</th>
						<th>Review Comments</th>
						<th>Reviewed By</th>
						<th></th>
                      </tr>
                    </thead>
                    
                    <tbody style="overflow-y: scroll;">
					
                        <tr ng-repeat="work in workList | filter:searchWork " ng-dblclick="dblClickWorkList()" 
						ng-click="setSelected(work)" class="{{selected}}">
                            <td style="width: 10%">{{work.id}}</td>
                            <td style="width: 20%">{{work.name}}</td>
                            <td style="width: 20%">{{work.programName}}</td>
                            <td ng-class="{rejectColor: work.status == 'Re-work', approveColor: work.status == 'Approved', reviewColor: work.status == 'Review', inProgressColor: work.status == 'In-Progress'}">
							{{work.status}}</td>
                            <td>{{work.reviewComments}}</td>
                            <td>{{work.reviewedBy}}</td>
							<td class="TableImgsWork">							
							<img src="images/ListView_icon.png" 
                                  ng-if="work.status =='Approved'"
								 style="float:right;"
                                 class="material-icons" data-toggle="tooltip" data-placement="right" title="open in Editor" 
								 onMouseOver="this.style.cursor='pointer'" 
								 ng-click="measureConfigImgClick(work.id, work.name,work.status)" 
                                 > 
							
							<img src="images/Tickmark_green.jpg" 
								 style="float:right;" width="25" height="25"
                                 ng-if="work.status =='Review'"
                                 class="material-icons" data-toggle="tooltip" data-placement="right" title="Approve" 
								 onMouseOver="this.style.cursor='pointer'" 
								 ng-click="statusClickImg('Approved', work.id)"
                                > 
							
							<img src="images/Cross_Red.jpg" 
								 style="float:right;" width="25" height="25" 
                                 ng-if="work.status =='Review'"
                                 class="material-icons" data-toggle="tooltip" data-placement="right" title="Reject"
								 onMouseOver="this.style.cursor='pointer'" 
								 ng-click="statusClickImg('Re-work', work.id)"
                              >
							</td>		
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
    $(document).ready(function(){
    $('[data-toggle="tooltip"]').tooltip(); 


}); 
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