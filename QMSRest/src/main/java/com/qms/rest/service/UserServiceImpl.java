package com.qms.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.ResetPassword;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;

@Service("userService")
public class UserServiceImpl implements UserService {
	
	@Autowired
	private QMSConnection qmsConnection;	

	@Override
	public RestResult resetPassword(ResetPassword resetPassword) {
		Statement statement = null;
		PreparedStatement prepStatement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		RestResult restResult = null; 
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from QMS_USER_MASTER where USER_LOGINID='"+resetPassword.getUserId()+"'");
			String dbPassword = null;
			if (resultSet.next()) {
				dbPassword = resultSet.getString("PASSWORD");
				if(!dbPassword.equals(resetPassword.getOldPassword())) {
					restResult = RestResult.getFailRestResult(" Old password is not correct. ");
					return restResult; 
				}
			} 
			
			if(dbPassword == null) {
				restResult = RestResult.getFailRestResult(" Invalid user id. ");
				return restResult;				
			}
			
			if(!resetPassword.getNewPassword().equals(resetPassword.getConformPassword())) {
				restResult = RestResult.getFailRestResult(" New password and conform password should be same. ");
				return restResult;				
			}
			
			if(resetPassword.getNewPassword().equals(dbPassword)) {
				restResult = RestResult.getFailRestResult(" New password and Old password is same. ");
				return restResult;				
			}			
			
			String sqlStatementUpdate = "update QMS_USER_MASTER set PASSWORD=? where USER_LOGINID=?";
			
			prepStatement = connection.prepareStatement(sqlStatementUpdate);
			prepStatement.setString(1, resetPassword.getNewPassword());
			prepStatement.setString(2, resetPassword.getUserId());
			prepStatement.executeUpdate();
			
			restResult = RestResult.getSucessRestResult(" Reset password success.");
			
		} catch (Exception e) {
			e.printStackTrace();
			restResult = RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, null);
			qmsConnection.closeJDBCResources(null, prepStatement, connection);
		}
		return restResult;
	}
	
	@Override
	public User getUserInfo(String userName, String password) {
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		User user = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from QMS_USER_MASTER where USER_LOGINID='"+userName+"' and PASSWORD='"+password+"'");
			while (resultSet.next()) {
				user = new User();
				user.setEmail(resultSet.getString("USER_EMAIL"));
				user.setId(resultSet.getString("USER_ID"));
				user.setLoginId(resultSet.getString("USER_LOGINID"));
				user.setName(resultSet.getString("USER_NAME"));
				user.setRoleId(resultSet.getString("USER_ROLE_ID"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		
		return user;
	}	
	
}
