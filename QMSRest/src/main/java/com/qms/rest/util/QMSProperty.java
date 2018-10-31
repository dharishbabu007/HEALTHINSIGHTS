package com.qms.rest.util;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "qms")
@Component
public class QMSProperty {

	private String oracleJDBCUrl;
	private String oracleUserName;
	private String oraclePassword;
	
	private String oracleMeasureConfigJDBCUrl;
	private String oracleMeasureConfigUserName;
	private String oracleMeasureConfigPassword;	
	
	private String hiveJDBCUrl;
	private String hiveUserName;
	private String hivePassword;
	
	private String userStatusList;
	private String adminEMailId;	
	private int passwordExpiryTime;
	
	public String getUserStatusList() {
		return userStatusList;
	}
	public void setUserStatusList(String userStatusList) {
		this.userStatusList = userStatusList;
	}	
	public String getOracleJDBCUrl() {
		return oracleJDBCUrl;
	}
	public void setOracleJDBCUrl(String oracleJDBCUrl) {
		this.oracleJDBCUrl = oracleJDBCUrl;
	}
	public String getOracleUserName() {
		return oracleUserName;
	}
	public void setOracleUserName(String oracleUserName) {
		this.oracleUserName = oracleUserName;
	}
	public String getOraclePassword() {
		return oraclePassword;
	}
	public void setOraclePassword(String oraclePassword) {
		this.oraclePassword = oraclePassword;
	}
	public String getHiveJDBCUrl() {
		return hiveJDBCUrl;
	}
	public void setHiveJDBCUrl(String hiveJDBCUrl) {
		this.hiveJDBCUrl = hiveJDBCUrl;
	}
	public String getHiveUserName() {
		return hiveUserName;
	}
	public void setHiveUserName(String hiveUserName) {
		this.hiveUserName = hiveUserName;
	}
	public String getHivePassword() {
		return hivePassword;
	}
	public void setHivePassword(String hivePassword) {
		this.hivePassword = hivePassword;
	}
	public String getOracleMeasureConfigJDBCUrl() {
		return oracleMeasureConfigJDBCUrl;
	}
	public void setOracleMeasureConfigJDBCUrl(String oracleMeasureConfigJDBCUrl) {
		this.oracleMeasureConfigJDBCUrl = oracleMeasureConfigJDBCUrl;
	}
	public String getOracleMeasureConfigUserName() {
		return oracleMeasureConfigUserName;
	}
	public void setOracleMeasureConfigUserName(String oracleMeasureConfigUserName) {
		this.oracleMeasureConfigUserName = oracleMeasureConfigUserName;
	}
	public String getOracleMeasureConfigPassword() {
		return oracleMeasureConfigPassword;
	}
	public void setOracleMeasureConfigPassword(String oracleMeasureConfigPassword) {
		this.oracleMeasureConfigPassword = oracleMeasureConfigPassword;
	}
	public String getAdminEMailId() {
		return adminEMailId;
	}
	public void setAdminEMailId(String adminEMailId) {
		this.adminEMailId = adminEMailId;
	}
	public int getPasswordExpiryTime() {
		return passwordExpiryTime;
	}
	public void setPasswordExpiryTime(int passwordExpiryTime) {
		this.passwordExpiryTime = passwordExpiryTime;
	}
	
}
