package com.qms.rest.controller;

import java.util.Set;

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

import com.qms.rest.model.EMail;
import com.qms.rest.model.ResetPassword;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.SecurityQuestion;
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
	
	@RequestMapping(value = "/forgot_password", method = RequestMethod.POST)
	public ResponseEntity<RestResult> forgotPassword(@RequestBody EMail email, UriComponentsBuilder ucBuilder) {
		
		RestResult restResult = userService.forgotPassword(email.getEmailId());
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
		}

		return new ResponseEntity<RestResult>(restResult, HttpStatus.NOT_FOUND);
	}	
	
	@RequestMapping(value = "/login/{loginId}/{password}", method = RequestMethod.GET)
	public ResponseEntity<?> getUserDetails(@PathVariable("loginId") String loginId, @PathVariable("password") String password) {
		logger.info("Fetching user details for loginId {}", loginId);
		System.out.println("Fetching user details for loginId " + loginId);
		User user=null;
		try {
			user = userService.getUserInfo(loginId, password);
			if(user != null) {
				httpSession.setAttribute(QMSConstants.SESSION_USER_OBJ, user);
				System.out.println("Returned user name for loginId " + loginId + " : " + user.getFirstName());
				return new ResponseEntity<User>(user, HttpStatus.OK);
			} else {
				return new ResponseEntity(new CustomErrorType("User details not found. "), HttpStatus.NOT_FOUND);	
			}
		} catch (Exception e) {
			return new ResponseEntity(new CustomErrorType(e.getMessage()), HttpStatus.CONFLICT);
		}
//		if (user == null) {
//			logger.error("User details with loginId {} not found.", loginId);
//			return new ResponseEntity(new CustomErrorType("User details with loginId " + loginId 
//					+ " not found"), HttpStatus.NOT_FOUND);
//		} else {
//			httpSession.setAttribute(QMSConstants.SESSION_USER_OBJ, user);
//		}
//		
//		return new ResponseEntity<User>(user, HttpStatus.OK);
	}
	

	@RequestMapping(value = "/get_user/{loginId}", method = RequestMethod.GET)
	public ResponseEntity<?> getUserDetails(@PathVariable("loginId") String loginId) {
		System.out.println("Fetching user details for loginId " + loginId);
		User user = userService.getUserInfo(loginId);
		if (user == null) {
			return new ResponseEntity(new CustomErrorType("User details with loginId " + loginId 
					+ " not found"), HttpStatus.NOT_FOUND);
		} else {
			httpSession.setAttribute(QMSConstants.SESSION_USER_OBJ, user);
		}
		System.out.println("Returned user name for loginId " + loginId + " : " + user.getName());
		return new ResponseEntity<User>(user, HttpStatus.OK);
	}
	

	@RequestMapping(value = "/get_user_by_id/{userId}", method = RequestMethod.GET)
	public ResponseEntity<?> getUserDetailsByUserId(@PathVariable("userId") int userId) {
		System.out.println("Fetching user details for loginId " + userId);
		User user = userService.getUserInfoByUserId(userId);
		if (user == null) {
			return new ResponseEntity(new CustomErrorType("User details with loginId " + userId 
					+ " not found"), HttpStatus.NOT_FOUND);
		} else {
			httpSession.setAttribute(QMSConstants.SESSION_USER_OBJ, user);
		}
		System.out.println("Returned user name for loginId " + userId + " : " + user.getName());
		return new ResponseEntity<User>(user, HttpStatus.OK);
	}	

	
	@RequestMapping(value = "/create_user", method = RequestMethod.POST)
	public ResponseEntity<RestResult> createUser(@RequestBody User user, UriComponentsBuilder ucBuilder) {
		
		RestResult restResult = userService.addUser(user);
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
		}

		return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}
	
	@RequestMapping(value = "/update_user", method = RequestMethod.POST)
	public ResponseEntity<RestResult> updateUser(@RequestBody User user, UriComponentsBuilder ucBuilder) {
		
		RestResult restResult = userService.updateUser(user);
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
		}

		return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}
	
	@RequestMapping(value = "/get_securityQuestions", method = RequestMethod.GET)
	public ResponseEntity<Set<SecurityQuestion>> getSecurityQuestions() {
		System.out.println("Fetching SecurityQuestions ");
		Set<SecurityQuestion> questions = userService.getSecurityQuestions();
		return new ResponseEntity<Set<SecurityQuestion>>(questions, HttpStatus.OK);
	}	
}
