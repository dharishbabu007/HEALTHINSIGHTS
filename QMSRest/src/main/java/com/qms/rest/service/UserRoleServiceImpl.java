package com.qms.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.ScreenPermission;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RoleScreen;
import com.qms.rest.model.User;
import com.qms.rest.model.UserRole;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;

@Service("userRoleService123")
public class UserRoleServiceImpl implements UserRoleService{
	
	@Autowired
	private QMSConnection qmsConnection;
	
	@Autowired 
	private HttpSession httpSession;

	@Override
	public RestResult addUserRole(UserRole userRole) {

		Statement statement = null;
		PreparedStatement prepStatement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		RestResult restResult = null; 
		try {						
			connection = qmsConnection.getOracleConnection();
			
			String sqlStatementUpdate = "update QMS_USER_MASTER set USER_ROLE_ID=?, STATUS=? where USER_ID=?";
			prepStatement = connection.prepareStatement(sqlStatementUpdate);
			prepStatement.setInt(1, userRole.getRoleId());
			prepStatement.setString(2, userRole.getStatus());
			prepStatement.setInt(3, userRole.getUserId());
			prepStatement.executeUpdate();
			
			restResult = RestResult.getSucessRestResult(" User role mapping success.");
			
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
	public RestResult addRolescreens(RoleScreen rolePage) {
		PreparedStatement statement = null;
		PreparedStatement updateStatement = null;
		Connection connection = null;
		RestResult restResult = null;
		Statement statementObj = null;
		ResultSet resultSet = null;
		
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		try {
			connection = qmsConnection.getOracleConnection();
			connection.setAutoCommit(false);
			
			int roleId = rolePage.getRoleId();
			List<ScreenPermission> pagePermissions =  rolePage.getScreenPermissions();
			Set<Integer> existingPageIds = new HashSet<>();
			
			statementObj = connection.createStatement();
			resultSet = statementObj.executeQuery("select SCREEN_ID from QMS_ROLE_ACCESS where ROLE_ID="+roleId);
			while (resultSet.next()) {
				existingPageIds.add(resultSet.getInt("SCREEN_ID"));
			}						
			resultSet.close();		
			
			int maxPageRoleId = 0; 
			resultSet = statementObj.executeQuery("select max(ROLE_ACCESS_ID) from QMS_ROLE_ACCESS");
			if (resultSet.next()) {
				maxPageRoleId = resultSet.getInt(1);
			}						
			resultSet.close();			
			
			String sqlStatementInsert = "insert into QMS_ROLE_ACCESS(ROLE_ACCESS_ID,SCREEN_ID,ROLE_ID,READ,WRITE,DOWNLOAD,"
					+ "CURR_FLAG,REC_CREATE_DATE,REC_UPDATE_DATE,LATEST_FLAG,ACTIVE_FLAG,INGESTION_DATE,SOURCE_NAME,USER_NAME) "
					+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			statement = connection.prepareStatement(sqlStatementInsert);
			
			String sqlStatementUpdate = "update QMS_ROLE_ACCESS set READ=?,WRITE=?,DOWNLOAD=? where SCREEN_ID=? and ROLE_ID=?";
			updateStatement = connection.prepareStatement(sqlStatementUpdate);
			for (ScreenPermission pagePermissions2 : pagePermissions) {
				if(existingPageIds.contains(pagePermissions2.getScreenId())) {
					System.out.println(" Updating QMS_ROLE_ACCESS with role id and page id --> " + roleId + "," + pagePermissions2.getScreenId());
					int i = 0;
					updateStatement.setString(++i, pagePermissions2.getRead());
					updateStatement.setString(++i, pagePermissions2.getWrite());
					updateStatement.setString(++i, pagePermissions2.getDownload());
					updateStatement.setInt(++i, pagePermissions2.getScreenId());
					updateStatement.setInt(++i, roleId);
					updateStatement.addBatch();
				} else {
					int i=0;
					maxPageRoleId++;
					System.out.println(" Adding QMS_ROLE_ACCESS with id --> " + maxPageRoleId);
					statement.setInt(++i, maxPageRoleId);
					statement.setInt(++i, pagePermissions2.getScreenId());
					statement.setInt(++i, roleId);
					statement.setString(++i, pagePermissions2.getRead());
					statement.setString(++i, pagePermissions2.getWrite());
					statement.setString(++i, pagePermissions2.getDownload());
					
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
					
					statement.addBatch();
				}
			}
			int [] rowsAdded = statement.executeBatch();
			int [] rowsUpdated = updateStatement.executeBatch();
			connection.commit();
			System.out.println(" Rows updated --> " + rowsUpdated!=null?rowsUpdated.length:0);
			System.out.println(" Rows added   --> " + rowsAdded!=null?rowsAdded.length:0);
			restResult = RestResult.getSucessRestResult("Role pages mapping success.");
		} catch (Exception e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {				
				e1.printStackTrace();
			}
			restResult = RestResult.getFailRestResult(e.getMessage());
			e.printStackTrace();
		}
		finally {			
			qmsConnection.closeJDBCResources(resultSet, statementObj, null);
			qmsConnection.closeJDBCResources(null, updateStatement, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}	
		return restResult;
	}		

	@Override
	public UserRole getUserRole(String userId) {
		return null;
	}

	@Override
	public RoleScreen getRoleScreens(int roleId) {
		RoleScreen rolePage = new RoleScreen();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;

		try {						
			connection = qmsConnection.getPhoenixConnection();
			statement = connection.createStatement();
			
//			resultSet = statement.executeQuery("select USER_ROLE_ID from QMS_USER_MASTER where USER_ID="+userId);
//			int roleId = 0;
//			if (resultSet.next()) {
//				roleId = resultSet.getInt("USER_ROLE_ID");
//			}
//			resultSet.close();
			
			resultSet = statement.executeQuery("select * from QMS.QMS_ROLE_ACCESS where ROLE_ID="+roleId);
			rolePage.setRoleId(roleId);
			List<ScreenPermission> PagePermissions = new ArrayList<>();
			ScreenPermission pagePermission  = null;
			while (resultSet.next()) {
				pagePermission = new ScreenPermission();
				pagePermission.setScreenId(resultSet.getInt("SCREEN_ID"));
				pagePermission.setRead(resultSet.getString("READ"));
				pagePermission.setWrite(resultSet.getString("WRITE"));
				pagePermission.setDownload(resultSet.getString("DOWNLOAD"));	
				PagePermissions.add(pagePermission);
			}			
			rolePage.setScreenPermissions(PagePermissions);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		
		return rolePage;
	}
	
	//api to return all the pages
	//api to return all the users
	//api to return all the roles
	
	//api to return role & pages for user
	
	

}
