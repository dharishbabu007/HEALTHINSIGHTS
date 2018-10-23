package com.qms.rest.service;

import com.qms.rest.model.RestResult;
import com.qms.rest.model.RoleScreen;
import com.qms.rest.model.UserRole;

public interface UserRoleService {

	RestResult addUserRole(UserRole userRole);
	
	UserRole getUserRole(String userId);
	
	RestResult addRolePage(RoleScreen rolePage);
	
	RoleScreen getRolePage(String userId);	
}
