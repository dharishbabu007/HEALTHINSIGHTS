	function logOut() {	
		window.sessionStorage.clear();
		window.location.href="QMS_login.jsp";
	}
	window.onload = function() {		
		//if(window.sessionStorage.getItem("loginName") == "" || window.sessionStorage.getItem("loginName") == null)
		//	window.location.href="QMS_login.jsp";
		if(document.getElementById("userName") != null)
			document.getElementById("userName").innerHTML = sessionStorage.getItem("loginName");
	}