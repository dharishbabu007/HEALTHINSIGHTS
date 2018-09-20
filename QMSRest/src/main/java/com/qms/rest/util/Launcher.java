package com.qms.rest.util;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class Launcher {
	
	@Autowired
	HdfsAppendExample example;

    public void callLauncher() throws IOException {
        
        String coreSite = "/opt/hadoop_backup/etc/hadoop/core-site.xml";
        String hdfsSite = "/opt/hadoop_backup/etc/hadoop/hdfs-site.xml";
        FileSystem fileSystem = example.configureFileSystem(coreSite, hdfsSite);

        String hdfsFilePath = "hdfs://localhost:54310/UniqueEntry.csv";
        String res = example.appendToFile(fileSystem, "It's never too late" +
                " to start something good.", hdfsFilePath);

        if (res.equalsIgnoreCase( "success")) {
            System.out.println("Successfully appended to file");
            String content = example.readFromHdfs(fileSystem, hdfsFilePath);
            System.out.println(">>>>Content read from file<<<<\n" + content);
        }
        else
            System.out.println("couldn't append to file");

        example.closeFileSystem(fileSystem);
    }
}
