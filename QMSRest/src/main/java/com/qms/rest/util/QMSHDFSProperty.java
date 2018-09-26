package com.qms.rest.util;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "qmsHDFS")
@Component
public class QMSHDFSProperty {
	
	private String coreSite;
	private String hdfsSite;
	private String hdfsURL;
	private String hdfsFile;
	
	public String getCoreSite() {
		return coreSite;
	}
	public void setCoreSite(String coreSite) {
		this.coreSite = coreSite;
	}
	public String getHdfsSite() {
		return hdfsSite;
	}
	public void setHdfsSite(String hdfsSite) {
		this.hdfsSite = hdfsSite;
	}
	public String getHdfsURL() {
		return hdfsURL;
	}
	public void setHdfsURL(String hdfsURL) {
		this.hdfsURL = hdfsURL;
	}
	public String getHdfsFile() {
		return hdfsFile;
	}
	public void setHdfsFile(String hdfsFile) {
		this.hdfsFile = hdfsFile;
	}
	
	
}
