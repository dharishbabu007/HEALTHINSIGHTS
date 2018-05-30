package com.qms.rest.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.qms.rest.service.QMSServiceImpl;

@Component
public class QMSConnection {

	@Autowired
	private QMSProperty qmsProperty;
	
	public static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver"; //org.apache.hadoop.hive.jdbc.HiveDriver
	public static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.OracleDriver";
	
	public Connection getOracleConnection() throws Exception {
		
//		System.out.println("Oracle JDBC Url   --> " + qmsProperty.getOracleJDBCUrl());
//		System.out.println("Oracle User Name  --> " + qmsProperty.getOracleUserName());
//		System.out.println("Oracle Password() --> " + qmsProperty.getOraclePassword());
		//ORACLE
		Class.forName(ORACLE_JDBC_DRIVER);
		Connection connection = DriverManager.getConnection(qmsProperty.getOracleJDBCUrl(), qmsProperty.getOracleUserName(), qmsProperty.getOraclePassword());				
		return connection;
	}
	
	public Connection getOracleConnection(String schemaName, String password) throws Exception {
		Class.forName(ORACLE_JDBC_DRIVER);
		Connection connection = DriverManager.getConnection(qmsProperty.getOracleJDBCUrl(), schemaName, password);				
		return connection;
	}
	
	public Connection getOracleMeasureConfigConnection() throws Exception {
		Class.forName(ORACLE_JDBC_DRIVER);
		Connection connection = DriverManager.getConnection(qmsProperty.getOracleMeasureConfigJDBCUrl(), 
				qmsProperty.getOracleMeasureConfigUserName(), qmsProperty.getOracleMeasureConfigPassword());				
		return connection;
	}	
	
	public Connection getHiveConnection() throws Exception {
		//HIVE
		Class.forName(HIVE_JDBC_DRIVER);
		Connection connection = DriverManager.getConnection(qmsProperty.getHiveJDBCUrl(), qmsProperty.getHiveUserName(), qmsProperty.getHivePassword());		
		return connection;
	}
	
	public void closeJDBCResources (ResultSet resultSet, Statement statement, Connection connection) {
		try {
			if(resultSet != null) resultSet.close();
			if(statement != null) statement.close();
			if(connection != null) connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}	
	
}
