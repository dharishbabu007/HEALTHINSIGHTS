package com.qms.rest.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Component
public class Launcher {
	
	@Autowired
	HdfsAppendExample example;
	
	@Autowired
	QMSHDFSProperty qmsHDFSProperty; 

    public void callLauncher() throws IOException, URISyntaxException {
        
//        String coreSite = "/opt/hadoop_backup/etc/hadoop/core-site.xml";
//        String hdfsSite = "/opt/hadoop_backup/etc/hadoop/hdfs-site.xml";
//        FileSystem fileSystem = example.configureFileSystem(qmsHDFSProperty.getCoreSite(), qmsHDFSProperty.getHdfsSite());

        //String hdfsFilePath = "hdfs://localhost:54310/UniqueEntry.csv";
        String hdfsFilePath = qmsHDFSProperty.getHdfsURL()+qmsHDFSProperty.getHdfsFile();
//        System.out.println(" hdfsFilePath --------> " + hdfsFilePath);

        
//        String readContent = example.readFromHdfs(fileSystem, hdfsFilePath);
//        System.out.println(" ReadContent -----------> " + readContent);
        
        
        URI uri = URI.create (qmsHDFSProperty.getHdfsURL()+qmsHDFSProperty.getHdfsFile());
        Configuration conf = new Configuration ();
        FileSystem file = FileSystem.get (uri, conf);
        FSDataInputStream in = file.open(new Path(uri)); 
        
        if(in != null)
        System.out.println(" IIIIIIIIIIIIIIIIIn " + in.available());
        else
        System.out.println(" IIIIIIIIIIIIIIIIIn is NULL");

//      String res = example.appendToFile(fileSystem, "It's never too late" +
//      " to start something good.", hdfsFilePath);        

//        if (res.equalsIgnoreCase( "success")) {
//            System.out.println("Successfully appended to file");
//            String content = example.readFromHdfs(fileSystem, hdfsFilePath);
//            System.out.println(">>>>Content read from file<<<<\n" + content);
//        }
//        else
//            System.out.println("couldn't append to file");

//        example.closeFileSystem(fileSystem);
    }
}
