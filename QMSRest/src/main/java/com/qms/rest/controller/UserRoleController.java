package com.qms.rest.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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

import com.qms.rest.model.NameValue;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RoleScreen;
import com.qms.rest.model.UserRole;
import com.qms.rest.service.UserRoleService;
import com.qms.rest.util.QMSProperty;


@RestController
@RequestMapping("/user_role")
@CrossOrigin
public class UserRoleController {

	@Autowired
	UserRoleService userRoleService;	
	
	@Autowired
	private QMSProperty qmsProperty;
	
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
	
	@RequestMapping(value = "/get_role_screens/{roleId}", method = RequestMethod.GET)
	public ResponseEntity<RoleScreen> getRoleScreens(@PathVariable("roleId") int roleId) {		
		RoleScreen roleScreen = userRoleService.getRoleScreens(roleId);
		return new ResponseEntity<RoleScreen>(roleScreen, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/get_user_status_list", method = RequestMethod.GET)
	public ResponseEntity<List<String>> getUserStatusList() {
		
		String userStatusListStr = qmsProperty.getUserStatusList();		
		List<String> userStatusList = new ArrayList<String>(Arrays.asList(userStatusListStr.split(",")));
				
		return new ResponseEntity<List<String>>(userStatusList, HttpStatus.OK);
	}	
	

	@RequestMapping(value = "/getScreensForRole/{roleId}", method = RequestMethod.GET)
	public ResponseEntity<Set<NameValue>> getScreensForRole(@PathVariable("roleId") String roleId) {
		System.out.println("Fetching User Access Role ");
		Set<NameValue> favourites = userRoleService.getScreensForRole(roleId);
		return new ResponseEntity<Set<NameValue>>(favourites, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/updateRoleFavourites", method = RequestMethod.POST)
	public ResponseEntity<RestResult> updateRoleFavourites(@RequestBody RoleScreen rolePage, UriComponentsBuilder ucBuilder) {
		
		RestResult restResult = userRoleService.updateRoleFavourites(rolePage);
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
		}

		return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}	
	
}
