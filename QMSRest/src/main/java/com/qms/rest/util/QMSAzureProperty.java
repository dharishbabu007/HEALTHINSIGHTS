package com.qms.rest.util;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@ConfigurationProperties(prefix = "qmsAzure")
@Component
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class QMSAzureProperty {
	private String containerName;
	private String connectString;
	//models	
	private String uploadPathNoshow;	
	private String uploadPathLHE;
	private String uploadPathLHC;		
	private String uploadPathPersona;
	private String uploadPathNC;	
}
