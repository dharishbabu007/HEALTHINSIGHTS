package com.qms.rest.service;

import com.qms.rest.model.CSVOutPut;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.RestResult;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;

@Service("importExportService")
public class ImportExportServiceImpl implements ImportExportService {
	
	public static final Logger log = LoggerFactory.getLogger(ImportExportServiceImpl.class);
	
	String hostname = "192.168.184.74";
    String username = "Ashwinth";
    String password = "Login$321";

    // String copyWinFrom = "d:/fromWindows.del";
    String copyWinFrom = ""; //createTestFile("fromWindows.del").getAbsolutePath();
    String linuxImportPath = "/home/Ashwinth/input";
    String linuxExportPath = "/home/Ashwinth/output";
    String windowsCopyPath = "D:/import_export";	
    String rScriptFile = "/home/Ashwinth/script";

	@Override
	public RestResult importFile(String fileName) {
		try {
			putFile(hostname, username, password, fileName, linuxImportPath);
			return RestResult.getSucessRestResult(" File import success. ");
		} catch (JSchException | SftpException e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
	}

	@Override
	public RestResult exportFile(String fileName) {
		try {
			getFile(hostname, username, password, linuxExportPath, windowsCopyPath);
			return RestResult.getSucessRestResult(" File export success. ");
		} catch (JSchException e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
	}

	@Override
	public RestResult runRFile(String fileName) {
		try {
			Runtime.getRuntime().exec("Rscript "+fileName);
			return RestResult.getSucessRestResult(" RFile execution success. ");
		} catch (IOException e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		} 		
	}
	
    private void putFile(String hostname, String username, String password, String copyFrom, String copyTo)
            throws JSchException, SftpException {
        System.out.println("Initiate sending file to Linux Server...");
        JSch jsch = new JSch();
        Session session = null;
        System.out.println("Trying to connect.....");
        session = jsch.getSession(username, hostname, 22);
        session.setConfig("StrictHostKeyChecking", "no");
        session.setPassword(password);
        session.connect();
        System.out.println("is server connected? " + session.isConnected());

        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp sftpChannel = (ChannelSftp) channel;
        System.out.println("Server's home directory: " + sftpChannel.getHome());
        try {
            sftpChannel.put(copyFrom, copyTo, monitor, ChannelSftp.OVERWRITE);
            //sftpChannel.rm("input.csv");
            //sftpChannel.rename("", "");
        } catch (SftpException e) {
            //log.error("file was not found: " + copyFrom);
        	e.printStackTrace();
        }

        sftpChannel.exit();
        session.disconnect();
        System.out.println("Finished sending file to Linux Server...");
    }

    private void getFile(String hostname, String username, String password, String copyFrom, String copyTo)
            throws JSchException {
        System.out.println("Initiate getting file from Linux Server...");
        JSch jsch = new JSch();
        Session session = null;
        System.out.println("Trying to connect.....");
        session = jsch.getSession(username, hostname, 22);
        session.setConfig("StrictHostKeyChecking", "no");
        session.setPassword(password);
        session.connect();
        System.out.println("is server connected? " + session.isConnected());

        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp sftpChannel = (ChannelSftp) channel;
        try {
            System.out.println(sftpChannel.getHome());
        } catch (SftpException e1) {
            e1.printStackTrace();
        }
        try {
            sftpChannel.get(copyFrom, copyTo, monitor, ChannelSftp.OVERWRITE);            
//            sftpChannel.get(copyFrom+"/Output.csv", copyTo, monitor, ChannelSftp.OVERWRITE);
//            sftpChannel.get(copyFrom+"/ModelScore.csv", copyTo, monitor, ChannelSftp.OVERWRITE);
//            sftpChannel.get(copyFrom+"/ConfusionMatric.csv", copyTo, monitor, ChannelSftp.OVERWRITE);
//            sftpChannel.get(copyFrom+"/ModelSummary.csv", copyTo, monitor, ChannelSftp.OVERWRITE);
//            sftpChannel.get(copyFrom+"/ROCplot.PNG", copyTo, monitor, ChannelSftp.OVERWRITE);            
        } catch (SftpException e) {
            //log.error("file was not found: " + copyFrom);
        	e.printStackTrace();
        }

        sftpChannel.exit();
        session.disconnect();
        System.out.println("Finished getting file from Linux Server...");
    }
    
    final SftpProgressMonitor monitor = new SftpProgressMonitor() {
        public void init(final int op, final String source, final String target, final long max) {
            System.out.println("sftp start uploading file from:" + source + " to:" + target);
        }

        public boolean count(final long count) {
        	System.out.println("sftp sending bytes: " + count);
            return true;
        }

        public void end() {
            System.out.println("sftp uploading is done.");
        }
    };

	@Override
	public Set<CSVOutPut> getCSVOutPut() {
		
		Set<CSVOutPut> setOutput = new HashSet<>();
	    BufferedReader br = null;
		try {
			//getFile(hostname, username, password, linuxExportPath+"/Output.csv", windowsCopyPath);
			br = new BufferedReader(new FileReader(windowsCopyPath+"/Output.csv"));
		    int i = 0;
		    String line = null;
		    CSVOutPut output = null;
		    while ((line = br.readLine()) != null) {
		    	i++;
		    	if(i == 1) continue;
		    	String[] values = line.split(",");
		    	if(values.length > 17) {
			    	output = new CSVOutPut();			    
				    output.setAppointmentDay(values[7]);
				    output.setAppointmentID(values[2]);
				    output.setLikelihood(values[16]);
				    output.setNeighbourhood(values[9]);
				    output.setNoShow(values[17]);
				    output.setPatientId(values[0]);
				    output.setPatientName(values[1]);
				    //output.setScheduledDay(values[2]);
				    setOutput.add(output);
		    	}
		    	i++;
		    }		    
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(br != null) br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	    
	    
		return setOutput;
	}

	@Override
	public Set<ModelSummary> getCSVModelSummary() {
		Set<ModelSummary> setOutput = new HashSet<>();
	    BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(windowsCopyPath+"/ModelSummary.csv"));
		    String line = null;
		    ModelSummary output = null;
		    int i = 0;
		    while ((line = br.readLine()) != null) {
		    	i++;
		    	if(i == 1) continue;
		    	String[] values = line.split(",");
		    	if(values.length > 4) {
			    	output = new ModelSummary();			    
			    	output.setAttributes(values[0]);
			    	output.setEstimate(values[1]);
			    	output.setPrz(values[4]);
			    	//output.setSignificance(values[5]);
			    	output.setStdError(values[2]);
			    	output.setzValue(values[3]);
				    setOutput.add(output);
		    	}
		    }		    
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(br != null) br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return setOutput;
	}    
	

}
