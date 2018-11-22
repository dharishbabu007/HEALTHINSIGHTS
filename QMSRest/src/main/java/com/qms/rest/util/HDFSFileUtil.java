package com.qms.rest.util;

import java.net.URI;
import java.util.logging.Logger;

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

@Component
public class HDFSFileUtil {
	
	private static final Logger logger = Logger.getLogger("com.qms.rest.util.HDFSFileUtil");
	
	@Autowired
	QMSHDFSProperty qmsHDFSProperty;	
	
	public String createSubFolder(int fileId) throws Exception {
		FileSystem file=null;
		try {
			String hdfsFilePath = qmsHDFSProperty.getHdfsURL()+qmsHDFSProperty.getWritePath()+fileId;
			System.out.println(" hdfsFilePath createSubFolder --> "+hdfsFilePath);
	        URI uri = URI.create (hdfsFilePath);
	        Path dirPath = new Path(uri);
	        Configuration conf = new Configuration();
	        conf.set("fs.defaultFS", qmsHDFSProperty.getHdfsURL());
	        file = FileSystem.get (uri, conf);	
	        if(!file.exists(dirPath)) {
	        	System.out.println(" Creating directory.. " + hdfsFilePath);        	
	        	file.mkdirs(dirPath);
	        	System.out.println(" Dir created");
	        }
	        return hdfsFilePath;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
		finally {
			if(file != null) file.close();
		}		
	}
	
			
	public void putFile (MultipartFile uploadFile, int fileId) throws Exception {
		FSDataOutputStream outputStream=null;
		FileSystem file=null;
		System.setProperty("HADOOP_USER_NAME", qmsHDFSProperty.getHdfsUser());		
		try {
			String extension = FilenameUtils.getExtension(uploadFile.getOriginalFilename());
			String hdfsFilePath = createSubFolder(fileId)+"/"+fileId+"."+extension;
			//String hdfsFilePath = qmsHDFSProperty.getHdfsURL()+qmsHDFSProperty.getWritePath()+uploadFile.getOriginalFilename();
			System.out.println(" Writing the actual file to HDFS --> " + uploadFile.getOriginalFilename());			
	        //String hdfsFilePath = qmsHDFSProperty.getHdfsURL()+qmsHDFSProperty.getWritePath()+fileId+"."+extension;
	        URI uri = URI.create (hdfsFilePath);
	        Configuration conf = new Configuration();
	        conf.set("fs.defaultFS", qmsHDFSProperty.getHdfsURL());
	        file = FileSystem.get (uri, conf);		
			outputStream=file.create(new Path(uri));
			outputStream.write(uploadFile.getBytes());
			System.out.println("End Write file into hdfs"+hdfsFilePath);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
		finally {
			if(outputStream != null) outputStream.close();
			if(file != null) file.close();
		}
	}
	

//	public void putFilesInAzure() {
//		try {
//			Configuration conf = new Configuration();
//	        String hdfsUri = "hdfs://Healthinsight-HDI-ssh.azurehdinsight.net/";
//	        conf.set("fs.defaultFS", hdfsUri);
//	        FileSystem fileSystem;			
//			fileSystem = FileSystem.get(URI.create(hdfsUri), conf);
//			System.out.println("fileSystem --> " + fileSystem);
//	        RemoteIterator<LocatedFileStatus> fileStatusIterator = fileSystem.listFiles(new Path("/tmp"), true);
//	        while(fileStatusIterator.hasNext()) {
//	            System.out.println(" Files in azure" + fileStatusIterator.next().getPath().toString());
//	        }			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	
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
