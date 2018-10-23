package com.qms.rest.controller;

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

import com.qms.rest.model.RestResult;
import com.qms.rest.model.RoleScreen;
import com.qms.rest.model.UserRole;
import com.qms.rest.service.UserRoleService;


@RestController
@RequestMapping("/user_role")
@CrossOrigin
public class UserRoleController {

	@Autowired
	UserRoleService userRoleService;	
	
	@RequestMapping(value = "/add_user_role", method = RequestMethod.POST)
	public ResponseEntity<RestResult> addUserRole(@RequestBody UserRole userRole, UriComponentsBuilder ucBuilder) {
		
		RestResult restResult = userRoleService.addUserRole(userRole);
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
		}

		return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}
	

	@RequestMapping(value = "/add_role_screens", method = RequestMethod.POST)
	public ResponseEntity<RestResult> addRolePage(@RequestBody RoleScreen rolePage, UriComponentsBuilder ucBuilder) {
		
		RestResult restResult = userRoleService.addRolescreens(rolePage);
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
		}

		return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}	
	
	@RequestMapping(value = "/get_role_screens/{userId}", method = RequestMethod.GET)
	public ResponseEntity<RoleScreen> getRoleScreens(@PathVariable("userId") int userId) {		
		RoleScreen roleScreen = userRoleService.getRoleScreens(userId);
		return new ResponseEntity<RoleScreen>(roleScreen, HttpStatus.OK);
	}	
	
}
