package com.qms.rest.util;

import java.io.IOException;
import java.net.URI;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.util.logging.Logger;

@Component
public class HDFSFileUtil {
	
	private static final Logger logger = Logger.getLogger("com.qms.rest.util.HDFSFileUtil");
	
	FileSystem fs;
	
	//@PostConstruct
    public void init() {		
		Configuration conf = new Configuration();
		String hdfsuri = "hdfs://hdp-master1.datalab.com:8020/";
		conf.set("fs.defaultFS", hdfsuri);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());		
		System.setProperty("HADOOP_USER_NAME", "hdp-hadoop");
		System.setProperty("hadoop.home.dir", "/");	
		try {
			fs = FileSystem.get(URI.create(hdfsuri), conf);
		} catch (IOException e) {			
			e.printStackTrace();
		}		
    }
	
	public void createSubFolder() throws IOException {
		//==== Create folder if not exists
		Path workingDir=fs.getWorkingDirectory();
		System.out.println("workingDir.getName() --> " + workingDir.getName());
		Path newFolderPath= new Path("/user/hdp-hadoop/curis");
		if(!fs.exists(newFolderPath)) {
		   // Create new Directory
		   System.out.println(" Creating directory.. ");	
		   fs.mkdirs(newFolderPath);
		   System.out.println(" Dir created");
		}	
		System.out.println(" Dir created");
	}
		
	public void putFile (MultipartFile file) throws IOException {
		//==== Write file
		System.out.println("Begin Write file into hdfs");
		//Create a path
		Path hdfswritepath = new Path("/user/hdp-hadoop/curis/"+file.getOriginalFilename());
		//Init output stream
		FSDataOutputStream outputStream=fs.create(hdfswritepath);
		//Cassical output stream usage
		outputStream.writeBytes(new String(file.getBytes()));
		outputStream.close();
		System.out.println("End Write file into hdfs");
	}
	
	public void getFile () throws IOException {
		//==== Read file
		System.out.println("Read file from hdfs");
		//Create a path
		Path hdfsreadpath = new Path("");
		//Init input stream
		FSDataInputStream inputStream = fs.open(hdfsreadpath);
		//Classical input stream usage
		String out= IOUtils.toString(inputStream, "UTF-8");
		System.out.println(out);
		inputStream.close();
		fs.close();		
	}
	
//	public static void main(String[] args) {
//		HDFSFileUtil utils = new HDFSFileUtil();
//		
//		try {
//			utils.createSubFolder();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
}
