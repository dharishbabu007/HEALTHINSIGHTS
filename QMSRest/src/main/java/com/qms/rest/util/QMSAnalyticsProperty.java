package com.qms.rest.util;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@ConfigurationProperties(prefix = "qmsAnalytics")
@Component
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class QMSAnalyticsProperty {
	
	private String hostname;
	private String username;
	private String password;
	
	private String linuxUploadPath;
	private String linuxOutputPath;
	private String windowsCopyPath;
	private String linuxRScriptPath;
	
//	public String getLinuxUploadPath() {
//		return linuxUploadPath;
//	}
//	public void setLinuxUploadPath(String linuxUploadPath) {
//		this.linuxUploadPath = linuxUploadPath;
//	}
//	public String getLinuxOutputPath() {
//		return linuxOutputPath;
//	}
//	public void setLinuxOutputPath(String linuxOutputPath) {
//		this.linuxOutputPath = linuxOutputPath;
//	}
//	public String getWindowsCopyPath() {
//		return windowsCopyPath;
//	}
//	public void setWindowsCopyPath(String windowsCopyPath) {
//		this.windowsCopyPath = windowsCopyPath;
//	}
//	public String getLinuxRScriptPath() {
//		return linuxRScriptPath;
//	}
//	public void setLinuxRScriptPath(String linuxRScriptPath) {
//		this.linuxRScriptPath = linuxRScriptPath;
//	}
//	public String getHostname() {
//		return hostname;
//	}
//	public void setHostname(String hostname) {
//		this.hostname = hostname;
//	}
//	public String getUsername() {
//		return username;
//	}
//	public void setUsername(String username) {
//		this.username = username;
//	}
//	public String getPassword() {
//		return password;
//	}
//	public void setPassword(String password) {
//		this.password = password;
//	}
	
	
}
