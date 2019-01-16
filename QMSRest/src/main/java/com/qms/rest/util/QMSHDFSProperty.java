package com.qms.rest.util;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@ConfigurationProperties(prefix = "qmsHDFS")
@Component
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class QMSHDFSProperty {
	
	private String coreSite;
	private String hdfsSite;
	private String hdfsURL;
	private String readFile;
	private String writePath;
	private String hdfsUser;
	private String rapiNoShow;	
	private String rapiLHE;
	private String rapiLHC;
	
//	public String getrApiURL() {
//		return rApiURL;
//	}
//	public void setrApiURL(String rApiURL) {
//		this.rApiURL = rApiURL;
//	}
//	public String getCoreSite() {
//		return coreSite;
//	}
//	public void setCoreSite(String coreSite) {
//		this.coreSite = coreSite;
//	}
//	public String getHdfsSite() {
//		return hdfsSite;
//	}
//	public void setHdfsSite(String hdfsSite) {
//		this.hdfsSite = hdfsSite;
//	}
//	public String getHdfsURL() {
//		return hdfsURL;
//	}
//	public void setHdfsURL(String hdfsURL) {
//		this.hdfsURL = hdfsURL;
//	}
//	public String getWritePath() {
//		return writePath;
//	}
//	public void setWritePath(String writePath) {
//		this.writePath = writePath;
//	}
//	public String getReadFile() {
//		return readFile;
//	}
//	public void setReadFile(String readFile) {
//		this.readFile = readFile;
//	}
//	public String getHdfsUser() {
//		return hdfsUser;
//	}
//	public void setHdfsUser(String hdfsUser) {
//		this.hdfsUser = hdfsUser;
//	}	
	
	
}
