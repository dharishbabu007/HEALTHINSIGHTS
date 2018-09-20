package com.qms.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Set;
import java.util.HashSet;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.Mail;
import com.qms.rest.model.ResetPassword;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;
import com.qms.rest.util.PasswordGenerator;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;
import com.qms.rest.model.SecurityQuestion;

@Service("userService")
public class UserServiceImpl implements UserService {
	
	@Autowired
	private QMSConnection qmsConnection;	
	
	@Autowired
	private EmailService emailService;
	
	@Autowired 
	private HttpSession httpSession;	

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
			
			String sqlStatementUpdate = "update QMS_USER_MASTER set PASSWORD=?,RESET_PASSWORD=? where USER_LOGINID=?";
			
			prepStatement = connection.prepareStatement(sqlStatementUpdate);
			prepStatement.setString(1, resetPassword.getNewPassword());
			prepStatement.setString(2, "N");
			prepStatement.setString(3, resetPassword.getUserId());
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
			if(password != null)
				resultSet = statement.executeQuery("select * from QMS_USER_MASTER where USER_LOGINID='"+userName+"' and PASSWORD='"+password+"'");
			else
				resultSet = statement.executeQuery("select * from QMS_USER_MASTER where USER_LOGINID='"+userName+"'");
			while (resultSet.next()) {
				user = new User();
				user.setEmail(resultSet.getString("USER_EMAIL"));
				user.setId(resultSet.getString("USER_ID"));
				user.setLoginId(resultSet.getString("USER_LOGINID"));
				//user.setName(resultSet.getString("USER_NAME"));
				user.setRoleId(resultSet.getString("USER_ROLE_ID"));
				user.setFirstName(resultSet.getString("FIRST_NAME"));
				user.setLastName(resultSet.getString("LAST_NAME"));
				user.setPassword(resultSet.getString("PASSWORD"));
				user.setPhoneNumber(resultSet.getString("PHONE_NO"));
				user.setSecurityAnswer(resultSet.getString("SECURITY_ANSWER"));
				user.setSecurityQuestion(resultSet.getString("SECURITY_QUESTION"));
				user.setResetPassword(resultSet.getString("RESET_PASSWORD"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		
		return user;
	}

	@Override
	public RestResult addUser(User user) {		
		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = null;
		Statement statementObj = null;
		ResultSet resultSet = null;
		
		User getUser = getUserInfo(user.getLoginId());
		if(getUser != null) {
			return RestResult.getFailRestResult("Login Id already exists. Please enter unique id.");
		}
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		try {
			connection = qmsConnection.getOracleConnection();	
			
			statementObj = connection.createStatement();			
			resultSet = statementObj.executeQuery("select * from QMS_USER_MASTER where USER_EMAIL='"+user.getEmail()+"'");
			if (resultSet.next()) {
				return RestResult.getFailRestResult("Email id already exists. Please enter unique email.");
			}						
			resultSet.close();			

			resultSet = statementObj.executeQuery("select max(USER_ID) from QMS_USER_MASTER");
			int userId = 0;
			while (resultSet.next()) {
				userId = resultSet.getInt(1)+1;
			}						
			resultSet.close();			
			System.out.println(" Adding the user with user id --> " + userId);
			
			String sqlStatementInsert = "insert into QMS_USER_MASTER(USER_LOGINID,FIRST_NAME,LAST_NAME,SECURITY_QUESTION,"
					+ "SECURITY_ANSWER,PHONE_NO,USER_EMAIL,PASSWORD,USER_ROLE_ID,USER_ID,"
					+ "CURR_FLAG,REC_CREATE_DATE,REC_UPDATE_DATE,LATEST_FLAG,"
					+ "ACTIVE_FLAG,INGESTION_DATE,SOURCE_NAME,USER_NAME) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			statement = connection.prepareStatement(sqlStatementInsert);
			int i=0;
			statement.setString(++i, user.getLoginId());
			statement.setString(++i, user.getFirstName());	
			statement.setString(++i, user.getLastName());
			statement.setString(++i, user.getSecurityQuestion());
			statement.setString(++i, user.getSecurityAnswer());
			statement.setString(++i, user.getPhoneNumber());
			statement.setString(++i, user.getEmail());
			statement.setString(++i, user.getPassword());
			statement.setString(++i, QMSConstants.DEFAULT_USER_ROLE_ID);
			statement.setInt(++i, userId);
			
			Date date = new Date();				
			Timestamp timestamp = new Timestamp(date.getTime());				
			statement.setString(++i, "Y");
			statement.setTimestamp(++i, timestamp);
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i, "Y");
			statement.setString(++i, "A");
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i, QMSConstants.MEASURE_SOURCE_NAME);				
			
			if(userData == null || userData.getId() == null)
				statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
			else
				statement.setString(++i, userData.getLoginId());			
			
			statement.executeUpdate();
			restResult = RestResult.getSucessRestResult("User added successfully.");
		} catch (Exception e) {
			restResult = RestResult.getFailRestResult(e.getMessage());
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statementObj, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}	
		return restResult;		

	}

	@Override
	public RestResult updateUser(User user) {
		
		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = null;
		Statement statementObj = null;
		ResultSet resultSet = null;
		try {	
			
			if(user.getLoginId() == null || user.getEmail() == null) {
				return RestResult.getFailRestResult(" User Login Id and Email Id should be not be null. ");
			}
			
			connection = qmsConnection.getOracleConnection();	
			
			statementObj = connection.createStatement();			
			resultSet = statementObj.executeQuery("select * from QMS_USER_MASTER where USER_EMAIL='"+user.getEmail()+
					"' AND USER_LOGINID <> '"+user.getLoginId()+"'");
			if (resultSet.next()) {
				return RestResult.getFailRestResult("Email id already exists. Please enter another one.");
			}						
			resultSet.close();			
			
			
			String sqlStatementInsert = "update QMS_USER_MASTER set FIRST_NAME=?, LAST_NAME=?, SECURITY_QUESTION=?, "
					+ "SECURITY_ANSWER=?, PHONE_NO=?, USER_EMAIL=? WHERE USER_LOGINID=?";		
			statement = connection.prepareStatement(sqlStatementInsert);
			int i=0;							
			statement.setString(++i, user.getFirstName());	
			statement.setString(++i, user.getLastName());
			statement.setString(++i, user.getSecurityQuestion());
			statement.setString(++i, user.getSecurityAnswer());
			statement.setString(++i, user.getPhoneNumber());
			statement.setString(++i, user.getEmail());
			statement.setString(++i, user.getLoginId());			
			statement.executeUpdate();
			restResult = RestResult.getSucessRestResult("User updated successfully.");
		} catch (Exception e) {
			restResult = RestResult.getFailRestResult(e.getMessage());
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statementObj, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}	
		return restResult;
	}

	@Override
	public User getUserInfo(String userName) {	
		return getUserInfo(userName, null);
	}

	@Override
	public RestResult forgotPassword(String email) {
		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = null;
		Statement statementObj = null;
		ResultSet resultSet = null;
		try {	
			connection = qmsConnection.getOracleConnection();	
			
			statementObj = connection.createStatement();			
			resultSet = statementObj.executeQuery("select * from QMS_USER_MASTER where USER_EMAIL='"+email+"'");
			String userLoginId = null;
			if (resultSet.next()) {
				userLoginId = resultSet.getString("USER_LOGINID"); 				
			}						
			resultSet.close();			
			if(userLoginId == null) {
				return RestResult.getFailRestResult("User not found with the entered email id. Please enter valid email id.");
			}
			String temporaryPassword = PasswordGenerator.generatePassword();
			emailService.sendEmail(getForgotPasswordMail(email, temporaryPassword));
			
			String sqlStatementInsert = "update QMS_USER_MASTER set PASSWORD=?,RESET_PASSWORD=? WHERE USER_LOGINID=?";		
			statement = connection.prepareStatement(sqlStatementInsert);
			int i=0;							
			statement.setString(++i, temporaryPassword);			
			statement.setString(++i, "Y");
			statement.setString(++i, userLoginId);
			statement.executeUpdate();
			restResult = RestResult.getSucessRestResult("Temporary password sent to your email. "
					+ "Please reset your password after login. ");
		} catch (Exception e) {
			restResult = RestResult.getFailRestResult(e.getMessage());
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statementObj, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}	
		return restResult;		
	}
	
	private Mail getForgotPasswordMail (String email, String temporaryPassword) {
		Mail mail = new Mail();
		mail.setFrom("raghunadha.konda@itcinfotech.com");
		mail.setTo(email);
		mail.setSubject(" Healthin - Temporary password");
		String text = "<html><body>Your temporary password to login into the healthin application => <b>" + temporaryPassword + "</b><br>";
		text = text + "<br> Please reset your password after logged into the application.";		
		text = text + "<br> This is auto generated mail. Please do not reply to this mail.</body></html>";
		mail.setText(text);
		return mail;
	}

	@Override
	public Set<SecurityQuestion> getSecurityQuestions() {
		Set<SecurityQuestion> questions = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		SecurityQuestion securityQuestion = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery("select * from QMS_SECURITY_QUESTION order by SECURITY_QUESTION");
			while (resultSet.next()) {
				securityQuestion = new SecurityQuestion();
				securityQuestion.setId(resultSet.getInt("SECURITY_QUESTION"));
				securityQuestion.setQuestion(resultSet.getString("QUESTIONS"));
				questions.add(securityQuestion);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		
		return questions;		
	}
	
}
