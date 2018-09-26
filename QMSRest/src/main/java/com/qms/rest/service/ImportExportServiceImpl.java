package com.qms.rest.service;

import com.qms.rest.model.CSVOutPut;
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.ModelScore;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.RestResult;
import com.qms.rest.util.Launcher;
import com.qms.rest.util.QMSAnalyticsProperty;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;
import com.jcraft.jsch.ChannelExec;

@Service("importExportService")
public class ImportExportServiceImpl implements ImportExportService {
	
	public static final Logger log = LoggerFactory.getLogger(ImportExportServiceImpl.class);
	
	@Autowired
	private QMSAnalyticsProperty qmsAnalyticsProperty;
	
	String windowsCopyPath;
	
	@Autowired
	Launcher hdfsLauncher;

	@PostConstruct
    public void init() {
		windowsCopyPath = qmsAnalyticsProperty.getWindowsCopyPath();
    }	

	@Override
	public RestResult importFile(MultipartFile file) {
		try {			
			putFile(qmsAnalyticsProperty.getHostname(), qmsAnalyticsProperty.getUsername(), 
					qmsAnalyticsProperty.getPassword(), file, 
					qmsAnalyticsProperty.getLinuxUploadPath());
			
			return RestResult.getSucessRestResult(" File import success. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
	}

	@Override
	public RestResult exportFile(String fileName) {
		try {
			hdfsLauncher.callLauncher();
			//getFile(qmsAnalyticsProperty.getLinuxOutputPath());
			return RestResult.getSucessRestResult(" File export success. ");
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
	}

	@Override
	public RestResult runRFile(String modelType) {
//		String rFile = "Script_ITC_healthcare_6_9_2018_v0.R";
//		if(modelType.equalsIgnoreCase("model1")) {
//			rFile = "Script_ITC_healthcare_6_9_2018_v0.R";
//		} else if(modelType.equalsIgnoreCase("model1")) {
//			rFile = "Script_ITC_healthcare_6_9_2018_v0.R";
//		}
		String command1="Rscript "+qmsAnalyticsProperty.getLinuxRScriptPath();
		//String command1="ls -ltr";
		try {
			int channelExitStatus = -1;
			//Runtime.getRuntime().exec("Rscript "+modelType);
			java.util.Properties config = new java.util.Properties(); 
	    	config.put("StrictHostKeyChecking", "no");
	    	JSch jsch = new JSch();
	    	Session session=jsch.getSession(qmsAnalyticsProperty.getUsername(), qmsAnalyticsProperty.getHostname(), 22);
	    	session.setPassword(qmsAnalyticsProperty.getPassword());
	    	session.setConfig(config);
	    	session.connect();
	    	System.out.println("Connected");
	    	
	    	Channel channel=session.openChannel("exec");
	        ((ChannelExec)channel).setCommand(command1);
	        channel.setInputStream(null);
	        ((ChannelExec)channel).setErrStream(System.err);
	        
	        InputStream in=channel.getInputStream();
	        channel.connect();
	        byte[] tmp=new byte[1024];
	        System.out.println("*************RFILE Console begin******************");
	        while(true){
	          while(in.available()>0){
	            int i=in.read(tmp, 0, 1024);
	            if(i<0)break;
	            System.out.print(new String(tmp, 0, i));
	          }
	          if(channel.isClosed()){
	        	channelExitStatus = channel.getExitStatus();
	            System.out.println("exit-status: "+channel.getExitStatus());
	            break;
	          }
	          try{Thread.sleep(1000);}catch(Exception ee){}
	        }
	        System.out.println("*************RFILE Console end******************");
	        channel.disconnect();
	        session.disconnect();
			System.out.println("DONE R File execution. ");
			
			if(channelExitStatus == 0) {
				System.out.println("R File execution SUCCESS. ");
				getFile(qmsAnalyticsProperty.getLinuxOutputPath());
				System.out.println("Exported output files. ");
				return RestResult.getSucessRestResult(" RFile execution success. ");
			} else {
				System.out.println("R File execution FAILED. ");
				return RestResult.getFailRestResult(" RFile execution not success. ");
			}
		} catch (IOException | JSchException e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		} 
	}
	
    private void putFile(String hostname, String username, String password, MultipartFile copyFrom, String copyTo)
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
            //sftpChannel.put(copyFrom, copyTo, monitor, ChannelSftp.OVERWRITE);
        	sftpChannel.put(copyFrom.getInputStream(), copyTo+copyFrom.getOriginalFilename(), monitor, ChannelSftp.OVERWRITE);
        } catch (SftpException | IOException e) {
        	e.printStackTrace();
        }

        sftpChannel.exit();
        session.disconnect();
        System.out.println("Finished sending file to Linux Server...");
    }
    
    private void getFile(String copyFrom)
            throws JSchException {
        System.out.println("Initiate getting file from Linux Server...");
        JSch jsch = new JSch();
        Session session = null;
        System.out.println("Trying to connect.....");
        session = jsch.getSession(qmsAnalyticsProperty.getUsername(), qmsAnalyticsProperty.getHostname(), 22);
        session.setConfig("StrictHostKeyChecking", "no");
        session.setPassword(qmsAnalyticsProperty.getPassword());
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
            //sftpChannel.get(copyFrom, copyTo, monitor, ChannelSftp.OVERWRITE);            
            sftpChannel.get(copyFrom+"/Output.csv", windowsCopyPath, monitor, ChannelSftp.OVERWRITE);
            sftpChannel.get(copyFrom+"/ModelScore.csv", windowsCopyPath, monitor, ChannelSftp.OVERWRITE);
            sftpChannel.get(copyFrom+"/ConfusionMatric.csv", windowsCopyPath, monitor, ChannelSftp.OVERWRITE);
            sftpChannel.get(copyFrom+"/ModelSummary.csv", windowsCopyPath, monitor, ChannelSftp.OVERWRITE);
            sftpChannel.get(copyFrom+"/ROCplot.PNG", windowsCopyPath, monitor, ChannelSftp.OVERWRITE);
        	
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
			br = new BufferedReader(new FileReader(windowsCopyPath+"/Output.csv"));			
		    int i = 0;
		    String line = null;
		    CSVOutPut output = null;
		    while ((line = br.readLine()) != null) {
		    	System.out.println("data-" + line);
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

	@Override
	public Set<ConfusionMatric> getCSVConfusionMatric() {
		Set<ConfusionMatric> setOutput = new HashSet<>();
	    BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(windowsCopyPath+"/ConfusionMatric.csv"));
		    String line = null;
		    ConfusionMatric output = null;
		    int i = 0;
		    while ((line = br.readLine()) != null) {
		    	i++;
		    	if(i == 1) continue;
		    	String[] values = line.split(",");
		    	if(values.length > 2) {
			    	output = new ConfusionMatric();
			    	output.setId(values[0]);
			    	output.setZero(values[1]);
			    	output.setOne(values[2]);
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

	@Override
	public ModelScore getCSVModelScore() {
		ModelScore output = new ModelScore();
	    BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(windowsCopyPath+"/ModelScore.csv"));
		    String line = null;
		    
		    while ((line = br.readLine()) != null) {
		    	if(line != null && !line.trim().isEmpty()) {
			    	String[] values = line.split(",");
			    	output = new ModelScore();	
			    	output.setScore(values[0]);
			    	output.setImageFullPath(windowsCopyPath+"/ROCplot.PNG");
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
		return output;
	}    
	

}
