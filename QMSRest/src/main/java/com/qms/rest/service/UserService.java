package com.qms.rest.service;

import com.qms.rest.model.ResetPassword;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;

public interface UserService {

	RestResult resetPassword (ResetPassword resetPassword);
	
	User getUserInfo(String userName, String password);
	
}
