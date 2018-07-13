<!DOCTYPE html>
<html>

<head>
  <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
           <title>QMS Login</title>
    <script src="download/angular.min.js"></script>
	<script src="download/angular.js"></script>
	<script src="download/angular-route.js"></script>
	<script src="./Query_Builder_files/angular-sanitize.min.js"></script> 	
	<script src="qms_home.js"></script>	
	
<style>

  html,body{
    margin:0;
    height: 100%;
  }
    .wrapper {
       background: url("images/Curis_Login_bg.jpg");
        background-repeat: no-repeat;
    height:100%;
    position: relative;
    background-size: cover;
         
    }

.right {
       position: absolute;
    right: 0px;
    width: 300px;
    /* max-width: 300px; */
    /* border: 3px solid #73AD21; */
    padding: 10px;
    margin-right: 20px;
    bottom: 0px;
    margin-bottom: 58px;
}
   /*.container{
      margin-top: 65vh;
    max-width: 45vw;
    margin-left: 74vw;
    position: absolute;
    }*/

    input[type=text],
    input[type=password]
    {
         width: 258px;
        padding: 12px 20px;
        margin: 8px 0;
        display: inline-block;
        border: 1px solid #ccc;
        box-sizing: border-box;
    }
     #fp{
        float: left;
     }
     #su{
        float: right;
        margin-right: 50px;
     }
     
    button {
        background-color: #0D5889;
        color: white;
        padding: 14px 20px;
        margin: 8px 0;
        border: none;
        cursor: pointer;
        width: 100px;
         
    }
   button:hover {
        opacity: 0.8;
    }
    a{
      color: #fff;
      font-size: 15px;
    }
</style>
</head>
<body ng-app="QMSHomeManagement" ng-controller="LoginController">
   <div class="wrapper">
   
       <div class="container right">
           <form>
			 <font color="red"><b><span ng-bind="errorMessage"></span></b></font>
             <input type="text" ng-model="userName" placeholder="User ID" name="uname" required>
              <br>              
             <input type="password" ng-model="password" placeholder="Password" name="psw" required>
             <button ng-click="submit()">Login</button>
             <br>
             <input type="checkbox" checked="checked"> Remember me<br><br>
             <label id="fp"><b><a href="#">Forgot Password</a></b></label> 
             <label id="su"><b><a href="#">Sign Up</a></b></label>            
            </form>
       </div>
    </div>            
</body>

</html>
