package com.qms.rest.util;

import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.util.logging.Logger;

@Component
public class HDFSFileUtil {
	
	private static final Logger logger = Logger.getLogger("com.qms.rest.util.HDFSFileUtil");
	
	@Autowired
	QMSHDFSProperty qmsHDFSProperty;	
	
//	public void createSubFolder() throws IOException {
//		//==== Create folder if not exists
//		Path workingDir=fs.getWorkingDirectory();
//		System.out.println("workingDir.getName() --> " + workingDir.getName());
//		Path newFolderPath= new Path("/user/hdp-hadoop/curis");
//		if(!fs.exists(newFolderPath)) {
//		   // Create new Directory
//		   System.out.println(" Creating directory.. ");	
//		   fs.mkdirs(newFolderPath);
//		   System.out.println(" Dir created");
//		}	
//		System.out.println(" Dir created");
//	}
	
			
	public void putFile (MultipartFile uploadFile) throws Exception {
		FSDataOutputStream outputStream=null;
		FileSystem file=null;
		try {
	        String hdfsFilePath = qmsHDFSProperty.getHdfsURL()+qmsHDFSProperty.getWritePath()+uploadFile.getOriginalFilename();
	        URI uri = URI.create (hdfsFilePath);
			System.setProperty("HADOOP_USER_NAME", "hdp-hadoop");
	        Configuration conf = new Configuration();
	        file = FileSystem.get (uri, conf);		
	        System.out.println("Creating the file in hdfs --> "+hdfsFilePath);
			outputStream=file.create(new Path(uri));
			System.out.println("writing bytes size --> "+uploadFile.getBytes().length);
			outputStream.write(uploadFile.getBytes());
			System.out.println("End Write file into hdfs");
		} catch (Exception ex) {
			throw ex;
		}
		finally {
			if(outputStream != null) outputStream.close();
			if(file != null) file.close();
		}
	}
	
	public void getFile (String path) throws Exception {
		FSDataInputStream inputStream=null;
		FileSystem file=null;		
		try{
	        String hdfsFilePath = qmsHDFSProperty.getHdfsURL()+path;
	        URI uri = URI.create (hdfsFilePath);
	        Configuration conf = new Configuration();
	        file = FileSystem.get (uri, conf);
	        inputStream = file.open(new Path(uri));
			String out= IOUtils.toString(inputStream, "UTF-8");
			System.out.println(out);
			inputStream.close();
			file.close();
		} catch (Exception ex) {
			throw ex;
		}
		finally {
			if(inputStream != null) inputStream.close();
			if(file != null) file.close();
		}		
	}
	
}
