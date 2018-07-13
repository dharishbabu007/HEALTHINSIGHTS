<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>

<head>
   <title>Login Page</title>
  <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
<style>

  html,body{
    margin:0;
    height: 100%;
  }
    .wrapper {
       background: url("Curis_Login_bg.jpg");
        background-repeat: no-repeat;
    height:100%;
    position: relative;
    background-size: cover;
         
    }


   .container{
      margin-top: 65vh;
    max-width: 45vw;
    margin-left: 74vw;
    position: absolute;
    }

    input[type=text],
    input[type=password]
    {
        width: 20vw;
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
        margin-left: 10vw;
     }
     
    button {
        background-color: #0D5889;
        color: white;
        padding: 14px 20px;
        margin: 8px 0;
        border: none;
        cursor: pointer;
        width: 20vw;
         
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
<body>
   <div class="wrapper">
   
       <div class="container">
           <form>
           
             <input type="text" placeholder="User ID" name="uname" required>
              <br>
                        
             <input type="password" placeholder="Password" name="psw" required>
<!-- <a href="index12.html"> -->
             <button type="button" onclick="load_html1()">Login</button>
             <br>
              <input type="checkbox" checked="checked"> Remember me<br><br>
             <label id="fp"><b><a href="#">Forgot Password</a></b></label> 
             <label id="su"><b><a href="#">Sign Up</a></b></label>
            
            </form>
       </div>
    </div>
<script>
function load_html1() {

	window.location.href = "index12.html";
}
</script>            
</body>

</html>
