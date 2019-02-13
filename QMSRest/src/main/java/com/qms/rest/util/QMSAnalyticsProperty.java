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
	
	//private String linuxUploadPath;
	private String linuxOutputPath;
	private String windowsCopyPath;
	private String linuxRScriptPath;
	//models
	private String linuxUploadPathNoshow;	
	private String linuxUploadPathLHE;
	private String linuxUploadPathLHC;		
	private String linuxUploadPathPersona;
	private String linuxUploadPathNC;
	private String deploymentEnvironment;
	
}
