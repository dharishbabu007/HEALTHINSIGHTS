<!DOCTYPE html>
<html lang="en">

<head>
    <title>Member Details</title>  
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

<body  ng-app="QMSHomeManagement" ng-controller="MemberDetailsController">
  

        <div class="col-md-12 no-padding-margin main-content">
            <div class="sub-header">
                <p style="font-size: xx-large;float: left; margin-top: 5.5vh;color: white;margin-left: 3vw" id="heading"><b>Member Details</b></p>
                <div class="button-div">
                    <form class="search-form" class="form-inline" role="form" method="post" action="//www.google.com/search" target="_blank">
                        <div class="input-group">
                            <input type="text" class="form-control search-form" placeholder="Search" id="search">
                            <span class="input-group-btn">
								<button type="submit" class="btn btn-primary search-btn" data-target="#search-form" name="q" style="margin-top: 5px;padding: 0px;">
								<img src="SearchIcon.png" height= "33.5px">
								</button>
							</span>
                        </div>
                    </form>
                </div>
            </div>
            <div class="sub-content" id="sub-content">
                <table class="table library" id="tablesearch">
                    <thead>
                    <tr>
						<th style="width: 10%">Member Id</th>
						<th style="width: 20%">Name</th>
						<th>Age</th>
						<th>Gender</th>
						<th style="width: 10%">MRI Score</th>
						<th>Amount</th>
						<th style="width: 20%">Reason</th>
                    </tr>
                    </thead>
                    <tbody>
					   <tr ng-repeat="member in memberDetailsList" ng-click="viewMemberDetail(member)">
						<td style="width: 10%">{{member.id}}</font></td>
						<td style="width: 20%">{{member.name}}</font></td>
						<td>{{member.age}}</td>
						<td>{{member.gender}}</td>
						<td style="width: 10%">{{member.hccScore}}</td>
						<td>{{member.amount}}</td>        
						<td style="width: 20%">{{member.reason}}</td>        
					   </tr>					
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