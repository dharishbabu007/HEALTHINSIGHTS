package com.qms.rest.service;

import com.qms.rest.model.RestResult;
import com.qms.rest.model.RoleScreen;
import com.qms.rest.model.ScreenPermission;
import com.qms.rest.model.UserRole;

import java.util.Set;

import com.qms.rest.model.NameValue;

public interface UserRoleService {

	RestResult addUserRole(UserRole userRole);
	
	UserRole getUserRole(String userId);
	
	RestResult addRolescreens(RoleScreen rolePage);
	
	RoleScreen getRoleScreens(int roleId);
	
	String getRole(String roleId);
	
	Set<NameValue> getScreensForRole(String roleId);

	RestResult updateRoleFavourites(RoleScreen rolePage);
}
