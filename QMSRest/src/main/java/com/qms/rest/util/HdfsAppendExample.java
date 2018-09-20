package com.qms.rest.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.*;

@Component
public class HdfsAppendExample {

    /**
     * To configure the file system as per the Hadoop Configuration
     * @param coreSitePath Path to core-site.xml in hadoop
     * @param hdfsSitePath Path to hdfs-site.xml in hadoop
     * @return hadoop file system instance
     */
    public FileSystem configureFileSystem(String coreSitePath, String hdfsSitePath) {
        FileSystem fileSystem = null;
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("dfs.support.append", true);
            Path coreSite = new Path(coreSitePath);
            Path hdfsSite = new Path(hdfsSitePath);
            conf.addResource(coreSite);
            conf.addResource(hdfsSite);
            fileSystem = FileSystem.get(conf);
        } catch (IOException ex) {
            System.out.println("Error occurred while configuring FileSystem");
        }
        return fileSystem;
    }

    /**
     * appends content to hdfs file and also hflush the content to all the data nodes buffer
     * for availability.
     * @param fileSystem hadoop file system instance
     * @param content
     * @param dest
     * @return status
     * @throws IOException
     */
    public String appendToFile(FileSystem fileSystem, String content, String dest) throws IOException {

        Path destPath = new Path(dest);
        if (!fileSystem.exists(destPath)) {
            System.err.println("File doesn't exist");
            return "Failure";
        }

        Boolean isAppendable = Boolean.valueOf(fileSystem.getConf().get("dfs.support.append"));

        if(isAppendable) {
            FSDataOutputStream fs_append = fileSystem.append(destPath);
            PrintWriter writer = new PrintWriter(fs_append);
            writer.append(content);
            writer.flush();
            fs_append.hflush();
            writer.close();
            fs_append.close();
            return "Success";
        }
        else {
            System.err.println("Please set the dfs.support.append property to true");
            return "Failure";
        }
    }

    /**
     *
     * @param fileSystem
     * @param hdfsFilePath
     * @return file content
     */
    public String readFromHdfs(FileSystem fileSystem, String hdfsFilePath) {
        Path hdfsPath = new Path(hdfsFilePath);
        StringBuilder fileContent = new StringBuilder("");
        try{
            BufferedReader bfr=new BufferedReader(new InputStreamReader(fileSystem.open(hdfsPath)));
            String str;
            while ((str = bfr.readLine()) != null) {
                fileContent.append(str+"\n");
            }
        }
        catch (IOException ex){
            System.out.println("----------Could not read from HDFS---------\n");
        }
        return fileContent.toString();
    }

    /**
     * To close the opened file system
     * @param fileSystem
     */
    public void closeFileSystem(FileSystem fileSystem){
        try {
            fileSystem.close();
        }
        catch (IOException ex){
            System.out.println("----------Could not close the FileSystem----------");
        }
    }
}
