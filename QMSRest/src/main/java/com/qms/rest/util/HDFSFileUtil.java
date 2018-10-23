package com.qms.rest.util;

import java.net.URI;

import org.apache.commons.io.FilenameUtils;
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
	
	public String createSubFolder(int fileId) throws Exception {
		FileSystem file=null;
		try {
			String hdfsFilePath = qmsHDFSProperty.getHdfsURL()+qmsHDFSProperty.getWritePath()+fileId;		
	        URI uri = URI.create (hdfsFilePath);
	        Path dirPath = new Path(uri);
	        Configuration conf = new Configuration();
	        file = FileSystem.get (uri, conf);	
	        if(!file.exists(dirPath)) {
	        	System.out.println(" Creating directory.. " + hdfsFilePath);        	
	        	file.mkdirs(dirPath);
	        	System.out.println(" Dir created");
	        }
	        return hdfsFilePath;
		} catch (Exception ex) {
			throw ex;
		}
		finally {
			if(file != null) file.close();
		}		
	}
	
			
	public void putFile (MultipartFile uploadFile, int fileId) throws Exception {
		FSDataOutputStream outputStream=null;
		FileSystem file=null;
		System.setProperty("HADOOP_USER_NAME", "hdp-hadoop");		
		try {
			String extension = FilenameUtils.getExtension(uploadFile.getOriginalFilename());
			String hdfsFilePath = createSubFolder(fileId)+"/"+fileId+"."+extension;;
			//String hdfsFilePath = qmsHDFSProperty.getHdfsURL()+qmsHDFSProperty.getWritePath()+uploadFile.getOriginalFilename();
			System.out.println(" Writing the actual file to HDFS --> " + uploadFile.getOriginalFilename());			
	        //String hdfsFilePath = qmsHDFSProperty.getHdfsURL()+qmsHDFSProperty.getWritePath()+fileId+"."+extension;
	        URI uri = URI.create (hdfsFilePath);
	        Configuration conf = new Configuration();
	        file = FileSystem.get (uri, conf);		
			outputStream=file.create(new Path(uri));
			outputStream.write(uploadFile.getBytes());
			System.out.println("End Write file into hdfs"+hdfsFilePath);
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
