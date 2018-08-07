package com.qms.rest.controller;

import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.qms.rest.model.ResetPassword;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;
import com.qms.rest.service.UserService;
import com.qms.rest.util.CustomErrorType;
import com.qms.rest.util.QMSConstants;

@RestController
@RequestMapping("/user")
@CrossOrigin
public class UserController {
	
	@Autowired
	UserService userService;
	
	@Autowired 
	private HttpSession httpSession;	
	
	public static final Logger logger = LoggerFactory.getLogger(QMSController.class);	
	
	@RequestMapping(value = "/reset_password/", method = RequestMethod.POST)
	public ResponseEntity<RestResult> resetUserPassword(@RequestBody ResetPassword resetPassword, UriComponentsBuilder ucBuilder) {
		logger.info("Resetting password for user id : {}", resetPassword.getUserId());
		
		RestResult restResult = userService.resetPassword(resetPassword);
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
		}

		return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}	
	
	@RequestMapping(value = "/login/{loginId}/{password}", method = RequestMethod.GET)
	public ResponseEntity<?> getUserDetails(@PathVariable("loginId") String loginId, @PathVariable("password") String password) {
		logger.info("Fetching user details for loginId {}", loginId);
		System.out.println("Fetching user details for loginId " + loginId);
		User user = userService.getUserInfo(loginId, password);
		if (user == null) {
			logger.error("User details with loginId {} not found.", loginId);
			return new ResponseEntity(new CustomErrorType("User details with loginId " + loginId 
					+ " not found"), HttpStatus.NOT_FOUND);
		} else {
			httpSession.setAttribute(QMSConstants.SESSION_USER_OBJ, user);
		}
		System.out.println("Returned user name for loginId " + loginId + " : " + user.getName());
		return new ResponseEntity<User>(user, HttpStatus.OK);
	}	

}
