package com.qms.rest.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpSession;

import org.apache.commons.io.FilenameUtils;
import org.apache.derby.tools.sysinfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;
import com.qms.rest.model.CSVOutPut;
import com.qms.rest.model.CSVOutPut1;
import com.qms.rest.model.ComplianceOutPut;
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.FileUpload;
import com.qms.rest.model.ModelMetric;
import com.qms.rest.model.ModelScore;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.RestResult;
import com.qms.rest.repository.FileUpoadRepository;
import com.qms.rest.util.HDFSFileUtil;
import com.qms.rest.util.QMSAnalyticsProperty;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;
import com.qms.rest.util.QMSHDFSProperty;

@Service("importExportService")
public class ImportExportServiceImpl implements ImportExportService {
	
	public static final Logger log = LoggerFactory.getLogger(ImportExportServiceImpl.class);
	
	@Autowired
	private QMSAnalyticsProperty qmsAnalyticsProperty;
	
	@Autowired
	private FileUpoadRepository fileUpoadRepository;	
	
	String windowsCopyPath;
	
	@Autowired
	HDFSFileUtil hdfsFileUtil;
	
	@Autowired 
	private HttpSession httpSession;	
	
	@Autowired
	private QMSConnection qmsConnection;	
	
	@Autowired
	QMSHDFSProperty qmsHDFSProperty;

	@PostConstruct
    public void init() {
		windowsCopyPath = qmsAnalyticsProperty.getWindowsCopyPath();
    }	

	@Override
	public RestResult importFile(MultipartFile file, int fileId) {
		try {	
			//In Linux 			
			putFile(file, fileId);
			
			//In Hadoop
//			hdfsFileUtil.putFile(file, fileId);
			
			//In windows
//			createUploadFileInWindows(file, fileId);
			
			return RestResult.getSucessRestResult(" File upload success. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
	}

	@Override
	public RestResult exportFile(String fileName) {
		try {
			//hdfsFileUtil.getFile(fileName);
			
			getFile(qmsAnalyticsProperty.getLinuxOutputPath());
			return RestResult.getSucessRestResult(" File export success. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
	}
	
	private RestResult executeRInLinux (String modelType) {
		
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

	@Override
	public RestResult runRFile(String modelType) {
//		if(true)
//			return RestResult.getSucessRestResult(" RFile execution success. ");
		
		int fileId = 0;
		if(httpSession.getAttribute(QMSConstants.INPUT_FILE_ID) != null)
			fileId = (int) httpSession.getAttribute(QMSConstants.INPUT_FILE_ID);
		else
			return RestResult.getFailRestResult(" Input file id is null. ");
		
//		int processedFileId = 0;
//		if(httpSession.getAttribute("PROCESSED_FILE_ID") != null)
//			processedFileId = (int) httpSession.getAttribute("PROCESSED_FILE_ID");	
//		System.out.println(fileId + " is the File Id. PROCESSED_FILE_ID : " + processedFileId);
//		if(processedFileId == fileId) {
//			return RestResult.getFailRestResult(" Already processed/processing for file id : "+fileId);
//		}
//		httpSession.setAttribute("PROCESSED_FILE_ID", fileId);
		
		String rApiUrl = qmsHDFSProperty.getRapiURL();
		rApiUrl = rApiUrl.replaceAll("FILE_ID", fileId+"");
		System.out.println("Calling R API Url --> " + rApiUrl);
		RestTemplate restTemplate = new RestTemplate();		
		String result = restTemplate.getForObject(rApiUrl, String.class);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        System.out.println(" R API Rest Result --> " + result);
		if(result != null && result.contains("Completed")) {
			return RestResult.getSucessRestResult("R Script Execution Success");
		} else {
			return RestResult.getFailRestResult(" R Script Execution Failed");
		}
	}
	
    private void putFile(MultipartFile file, int fileId)
            throws JSchException, SftpException {
        System.out.println("Initiate sending file to Linux Server...");
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
        System.out.println("Server's home directory: " + sftpChannel.getHome());
        try {
        	System.out.println(" Creating the directory under --> " + qmsAnalyticsProperty.getLinuxUploadPath()+"/"+fileId);
            //sftpChannel.put(copyFrom, copyTo, monitor, ChannelSftp.OVERWRITE);        	
        	sftpChannel.mkdir(qmsAnalyticsProperty.getLinuxUploadPath()+"/"+fileId);
        	String extension = FilenameUtils.getExtension(file.getOriginalFilename());
        	String filePath = qmsAnalyticsProperty.getLinuxUploadPath()+"/"+fileId+"/"+fileId+"."+extension;
        	System.out.println(" Creating the file under --> " + filePath);
        	sftpChannel.put(file.getInputStream(), filePath, monitor, ChannelSftp.OVERWRITE);
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
		
//		RestResult result = runRFile("model1");		
//		System.out.println(" R API Output --> " + result.getMessage());
		Set<CSVOutPut> setOutput = new HashSet<>();
//		if(result != null && result.getMessage().contains("Completed!")) {
//		}
			
		int fileId = 0;
		if(httpSession.getAttribute(QMSConstants.INPUT_FILE_ID) != null)
			fileId = (int) httpSession.getAttribute(QMSConstants.INPUT_FILE_ID);				
		System.out.println(" Getting the data from ns_file_output for file id " + fileId);		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from ns_file_output where fid='"+fileId+"'");
			//resultSet = statement.executeQuery("select * from ns_file_output where fid='45' limit 500");
			CSVOutPut output = null;
			while (resultSet.next()) {
		    	output = new CSVOutPut();			    
			    output.setAppointmentDay(resultSet.getString("appointmentday"));
			    output.setAppointmentID(resultSet.getString("appointmentid"));
			    output.setLikelihood(resultSet.getString("logodds"));
			    output.setNeighbourhood(resultSet.getString("neighbourhood"));
			    output.setNoShow(resultSet.getString("predictednoshow"));
			    output.setPatientId(resultSet.getString("patientid"));
			    output.setPatientName(resultSet.getString("patientname"));
			    setOutput.add(output);
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		System.out.println(fileId+" Data returned from ns_file_output " + setOutput.size());
		
//	    BufferedReader br = null;
//		try {		
//			br = new BufferedReader(new FileReader(windowsCopyPath+"/Output100.csv"));			
//		    int i = 0;
//		    String line = null;
//		    CSVOutPut output = null;
//		    while ((line = br.readLine()) != null) {
//		    	i++;
//		    	if(i == 1) continue;
//		    	String[] values = line.split(",");
//		    	if(values.length > 17) {
//			    	output = new CSVOutPut();			    
//				    output.setAppointmentDay(values[7]);
//				    output.setAppointmentID(values[2]);
//				    output.setLikelihood(values[16]);
//				    output.setNeighbourhood(values[9]);
//				    output.setNoShow(values[17]);
//				    output.setPatientId(values[0]);
//				    output.setPatientName(values[1]);
//				    setOutput.add(output);
//		    	}
//		    	i++;
//		    }		    
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		finally {
//			try {
//				if(br != null) br.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
				
		return setOutput;
	}
	

	@Override
	public Set<CSVOutPut1> getCSVOutPut1() {
		
		Set<CSVOutPut1> setOutput = new HashSet<>();
		
		int fileId = 0;
		if(httpSession.getAttribute(QMSConstants.INPUT_FILE_ID) != null)
			fileId = (int) httpSession.getAttribute(QMSConstants.INPUT_FILE_ID);		
		System.out.println(" Getting OutPut1 results for file id --> " + fileId);
		
		//ORACLE
		Statement statement1 = null;
		ResultSet resultSet1 = null;		
		Connection connection1 = null;
		Map<String, String[]> memberIdMap = new HashMap<>();
		Map<String, String> openMemberIdMap = new HashMap<>();
		//String memberCregapListQry = "SELECT * FROM FINDMEMGAPLISTFORALL ORDER BY TIME_PERIOD DESC";
		String memberCregapListQry = "SELECT DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME) AS NAME, DM.GENDER,"
		+"FLOOR(TRUNC(CAST('31-DEC-18' AS DATE) - (TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')))/365.25) AS AGE,"
		+"(DP.FIRST_NAME||' '||DP.LAST_NAME) AS PCP, DQM.MEASURE_TITLE AS CARE_GAPS, GIC.STATUS,GIC.QUALITY_MEASURE_ID,"
		+"COUNT(DQM.MEASURE_TITLE) AS COUNT_OF_CARE_GAPS, DPP.PLAN_NAME AS PLAN, GIC.GAP_DATE AS TIME_PERIOD, GIC.COMPLIANCE_POTENTIAL "
		+"FROM QMS_GIC_LIFECYCLE GIC "
		+"INNER JOIN DIM_MEMBER DM ON DM.MEMBER_ID = GIC.MEMBER_ID "
		+"INNER JOIN FACT_MEM_ATTRIBUTION FMA ON FMA.MEMBER_SK = DM.MEMBER_SK "
		+"INNER JOIN DIM_PROVIDER DP ON DP.PROVIDER_SK = FMA.PROVIDER_SK "
		+"INNER JOIN DIM_QUALITY_MEASURE DQM ON DQM.QUALITY_MEASURE_ID = GIC.QUALITY_MEASURE_ID "
		+"INNER JOIN DIM_PRODUCT_PLAN DPP ON DPP.PRODUCT_PLAN_ID = GIC.PRODUCT_PLAN_ID "
		+"WHERE GIC.GAP_DATE <= SYSDATE "
		+"GROUP BY DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME), DM.GENDER,GIC.COMPLIANCE_POTENTIAL,"
		+"FLOOR(TRUNC(CAST('31-DEC-18' AS DATE) - (TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')))/365.25),"
		+"(DP.FIRST_NAME||' '||DP.LAST_NAME), DQM.MEASURE_TITLE, GIC.STATUS, DPP.PLAN_NAME, GIC.GAP_DATE,GIC.QUALITY_MEASURE_ID order by GIC.GAP_DATE DESC";		
		
		
		String memberId = null;
		try {						
			connection1 = qmsConnection.getOracleConnection();
			statement1 = connection1.createStatement();			
			resultSet1 = statement1.executeQuery(memberCregapListQry);
			while (resultSet1.next()) {
				memberId = resultSet1.getString("MEMBER_ID");
				if(!memberIdMap.containsKey(memberId)) {
					memberIdMap.put(memberId, new String[]{resultSet1.getString("STATUS"), 
							resultSet1.getString("COUNT_OF_CARE_GAPS")});
				} 
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet1, statement1, connection1);
		}
		
		Set<String> keySet = memberIdMap.keySet();
		for (String key : keySet) {
			String[] values = memberIdMap.get(key);
			if(values[0].contains("Open") || values[0].contains("open")) 
				openMemberIdMap.put(key, values[1]);
		}		
		
		memberIdMap.clear();
		System.out.println(" Opened member ids list --> " + openMemberIdMap.size());
		
		//HIVE
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from ns_file_output where fid='"+fileId+"' and predictednoshow='1'");
			CSVOutPut1 output = null;
			String patientId = null;
			while (resultSet.next()) {				
		    	output = new CSVOutPut1();		    	
		    	patientId = resultSet.getString("patientid");
		    	output.setAppointmentId(resultSet.getString("appointmentid"));
		    	output.setAge(resultSet.getString("age"));
		    	output.setAppointmentDay(resultSet.getString("appointmentday"));
		    	output.setDayClass(resultSet.getString("dayclass"));
		    	output.setGender(resultSet.getString("gender"));
		    	output.setLogOdds(resultSet.getString("logodds"));
		    	output.setNeighbourhood(resultSet.getString("neighbourhood"));
		    	output.setNoshow(resultSet.getString("predictednoshow"));
		    	output.setRiskGrade(this.getRiskBasedOnCareGap(openMemberIdMap.get(patientId)));
		    	output.setCountCareGaps(openMemberIdMap.get(patientId));
			    output.setPatientId(patientId);
			    output.setName(resultSet.getString("patientname"));
			    setOutput.add(output);
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}				
		System.out.println(" Results returned from HIVE --> " + setOutput.size());
		

//	    BufferedReader br = null;
//		try {		
//			br = new BufferedReader(new FileReader(windowsCopyPath+"/Output32.csv"));			
//		    int i = 0;
//		    String line = null;
//		    CSVOutPut1 output = null;
//		    while ((line = br.readLine()) != null) {
//		    	i++;
//		    	if(i == 1) continue;
//		    	String[] values = line.split(",");
//		    	if(values.length > 10 && values[9] !=null && values[9].trim().equalsIgnoreCase("1")) {
//		    		int counter=0;
//			    	output = new CSVOutPut1();	
//			    	output.setPatientId(values[counter++]);
//			    	output.setName(values[counter++]);
//			    	output.setAppointmentId(values[counter++]);
//			    	output.setGender(values[counter++]);
//			    	output.setDayClass(values[counter++]);
//			    	output.setAppointmentDay(values[counter++]);
//			    	output.setAge(values[counter++]);
//			    	output.setNeighbourhood(values[counter++]);
//			    	output.setLogOdds(values[counter++]);
//			    	output.setNoshow(values[counter++]);
//			    	output.setCountCareGaps(values[counter++]);
//			    	output.setRiskGrade(values[counter++]);
//				    setOutput.add(output);
//		    	}
//		    	i++;
//		    }		    
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		finally {
//			try {
//				if(br != null) br.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
	    
	    
		return setOutput;
	}
	
	private String getRiskBasedOnCareGap(String carGapStr) {
		if(carGapStr == null) return "LOW"; 
		int carGap = Integer.parseInt(carGapStr.trim());
		String risk = "LOW";
		if(carGap == 2 || carGap ==3) {
			risk = "MEDIUM";
		}else if(carGap > 3 && carGap < 7) {
			risk = "HIGH";
		}else if(carGap > 6) {
			risk = "CATASTROPHIC";
		}
		return risk;
	}	
	

	@Override
	public Set<ModelSummary> getCSVModelSummary() {
		Set<ModelSummary> setOutput = new HashSet<>();
		
		
//		int fileId = 0;
//		if(httpSession.getAttribute(QMSConstants.INPUT_FILE_ID) != null)
//			fileId = (int) httpSession.getAttribute(QMSConstants.INPUT_FILE_ID);		
//		System.out.println(" Getting the ns_model_summary data for file id " + fileId);
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from ns_model_summary where modelid='1'");			
			ModelSummary output = null;
			while (resultSet.next()) {
		    	output = new ModelSummary();			    
		    	output.setAttributes(resultSet.getString("attribute"));
		    	output.setEstimate(resultSet.getString("estimate"));
		    	output.setPrz(resultSet.getString("pvalue"));
		    	output.setStdError(resultSet.getString("stderror"));
		    	output.setzValue(resultSet.getString("zvalue"));
			    setOutput.add(output);
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		System.out.println(" ModelSummary records size --> " + setOutput.size());
		
		
//	    BufferedReader br = null;
//		try {
//			br = new BufferedReader(new FileReader(windowsCopyPath+"/ModelSummary.csv"));
//		    String line = null;
//		    ModelSummary output = null;
//		    int i = 0;
//		    while ((line = br.readLine()) != null) {
//		    	i++;
//		    	if(i == 1) continue;
//		    	String[] values = line.split(",");
//		    	if(values.length > 4) {
//			    	output = new ModelSummary();			    
//			    	output.setAttributes(values[0]);
//			    	output.setEstimate(values[1]);
//			    	output.setPrz(values[4]);
//			    	output.setStdError(values[2]);
//			    	output.setzValue(values[3]);
//				    setOutput.add(output);
//		    	}
//		    }		    
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		finally {
//			try {
//				if(br != null) br.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
		
		return setOutput;
	}

	@Override
	public Set<ConfusionMatric> getCSVConfusionMatric() {
		Set<ConfusionMatric> setOutput = new HashSet<>();
		
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from ns_model_metric where modelid='1'");
			while (resultSet.next()) {
				ConfusionMatric output1 = new ConfusionMatric();
		    	output1.setId(resultSet.getString("modelid"));
		    	output1.setZero(resultSet.getString("tp"));
		    	output1.setOne(resultSet.getString("tn"));
		    	System.out.println(resultSet.getString("tp") + " TP TN" + resultSet.getString("tn"));
			    setOutput.add(output1);
			    
			    ConfusionMatric output2 = new ConfusionMatric();
		    	output2.setId(resultSet.getString("modelid"));
		    	output2.setZero(resultSet.getString("fp"));
		    	output2.setOne(resultSet.getString("fn"));
		    	System.out.println(resultSet.getString("fp") + " FP FN" + resultSet.getString("fn"));
			    setOutput.add(output2);			    
			    
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		
		
//	    BufferedReader br = null;
//		try {
//			br = new BufferedReader(new FileReader(windowsCopyPath+"/ConfusionMatric.csv"));
//		    String line = null;
//		    ConfusionMatric output = null;
//		    int i = 0;
//		    while ((line = br.readLine()) != null) {
//		    	i++;
//		    	if(i == 1) continue;
//		    	String[] values = line.split(",");
//		    	if(values.length > 2) {
//			    	output = new ConfusionMatric();
//			    	output.setId(values[0]);
//			    	output.setZero(values[1]);
//			    	output.setOne(values[2]);
//				    setOutput.add(output);
//		    	}
//		    }		    
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		finally {
//			try {
//				if(br != null) br.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
		
		return setOutput;
	}

	@Override
	public ModelScore getCSVModelScore() {
		ModelScore output = new ModelScore();
				
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from ns_model_metric where modelid='1'");
			while (resultSet.next()) {
		    	output = new ModelScore();	
		    	output.setScore(resultSet.getString("score"));
		    	output.setImageFullPath(windowsCopyPath+"/ROCplot.PNG");
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		
//	    BufferedReader br = null;
//		try {
//			br = new BufferedReader(new FileReader(windowsCopyPath+"/ModelScore.csv"));
//		    String line = null;
//		    
//		    while ((line = br.readLine()) != null) {
//		    	if(line != null && !line.trim().isEmpty()) {
//			    	String[] values = line.split(",");
//			    	output = new ModelScore();	
//			    	output.setScore(values[0]);
//			    	output.setImageFullPath(windowsCopyPath+"/ROCplot.PNG");
//		    	}
//		    }		    
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		finally {
//			try {
//				if(br != null) br.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
		
		
		return output;
	}
	
	private void createUploadFileInWindows(MultipartFile uploadFile, int fileId) throws Exception {
		
		String extension = FilenameUtils.getExtension(uploadFile.getOriginalFilename());
		File inputDir = new File(qmsAnalyticsProperty.getWindowsCopyPath()+"/input/");
		if(!inputDir.exists()) {
			inputDir.mkdir();
			System.out.println(" input dir created..");
		}
		File fileIdDir = new File(qmsAnalyticsProperty.getWindowsCopyPath()+"/input/"+fileId);
		boolean dirCreated = fileIdDir.mkdir();
		System.out.println(dirCreated +" fileId dir created.." + fileIdDir.getAbsolutePath());
		if(dirCreated) {
			FileOutputStream out = new FileOutputStream(qmsAnalyticsProperty.getWindowsCopyPath()+"/input/"+fileId+"/"+fileId+"."+extension);
			out.write(uploadFile.getBytes());
			out.close();	
			System.out.println(" file created success in windows ..");
		}		
	}

	@Override
	public FileUpload saveFileUpload(FileUpload fileUpload) {
		Date currentDate  = new Date();
		fileUpload.setCurrentFlag("Y");
		fileUpload.setRecCreateDate(currentDate);
		fileUpload.setRecUpdateDate(currentDate);
		fileUpload.setLatestFlag("Y");
		fileUpload.setActiveFlag("Y");
		fileUpload.setIngestionDate(currentDate);
		fileUpload.setSource(QMSConstants.MEASURE_SOURCE_NAME);
		if(fileUpload.getUserName() == null) {
			fileUpload.setUserName(QMSConstants.MEASURE_USER_NAME);
		}		
		List<FileUpload> uploades = fileUpoadRepository.getFileUpoadByMaxFileId();
		
		int fileId = 1;
		if(uploades != null && !uploades.isEmpty()) {
			fileId = uploades.get(0).getFileId() + 1;
		}
		System.out.println(" Creating qms_file_input with fileId -->" + fileId);
		fileUpload.setFileId(fileId);
		return fileUpoadRepository.save(fileUpload);
	}

	@Override
	public RestResult callHivePatitioning() {
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		
		int fileId = 0;
		if(httpSession.getAttribute(QMSConstants.INPUT_FILE_ID) != null)
			fileId = (int) httpSession.getAttribute(QMSConstants.INPUT_FILE_ID);
		else
			return RestResult.getFailRestResult(" Input file id is null. ");		
		
		try {						
//			connection = qmsConnection.getHiveThriftConnection();
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();	
//			String hdfsInputLocation = "/"+qmsHDFSProperty.getWritePath()+fileId;			
//			statement.executeQuery("ALTER TABLE NS_FILE_INPUT ADD PARTITION (fid="+fileId+") LOCATION '"+hdfsInputLocation+"'");
			
			//String inputFilePath = qmsAnalyticsProperty.getWindowsCopyPath()+"/input/"+fileId+"/"+fileId+".csv";
			String inputFilePath = qmsAnalyticsProperty.getLinuxUploadPath()+"/"+fileId+"/"+fileId+".csv";
			System.out.println("Loading the file in hive --> " + inputFilePath);
			statement.executeUpdate("LOAD DATA LOCAL INPATH '"+inputFilePath+"' INTO TABLE NS_FILE_INPUT PARTITION (fid="+fileId+")");
			
			System.out.println(" Alter table success for file id --> " + fileId);
			return RestResult.getSucessRestResult(" File upload success. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
	}

	@Override
	public Set<ComplianceOutPut> getComplianceOutPut() {
		
		Set<ComplianceOutPut> setOutput = new HashSet<>();
		
	    BufferedReader br = null;
		try {		
			br = new BufferedReader(new FileReader(windowsCopyPath+"/compliance/Output_v2.csv"));			
		    int L = 0;
		    String line = null;
		    ComplianceOutPut output = null;
		    while ((line = br.readLine()) != null) {
		    	L++;
		    	if(L == 1) continue;
		    	String[] values = line.split(",");
		    	if(values.length > 17) {
		    		int i = 0;
			    	output = new ComplianceOutPut();			    
			    	output.setPatId(values[i++]);
			    	output.setPatName(values[i++]);
			    	output.setAge(values[i++]);
			    	output.setGender(values[i++]);
			    	output.setRace(values[i++]);
			    	output.setEthnicity(values[i++]);
			    	output.setMartialStatus(values[i++]);
			    	output.setHaveHighSchoolDegreeYarn(values[i++]);
			    	output.setDisabolityYorn(values[i++]);
			    	output.setDistenceFormNear(values[i++]);
			    	output.setState(values[i++]);
			    	output.setZipCode(values[i++]);
			    	output.setCountry(values[i++]);
			    	output.setPcpAssignYorn(values[i++]);
			    	output.setEmpYorn(values[i++]);
			    	output.setInsuYorn(values[i++]);
			    	output.setNoOfdepts(values[i++]);
			    	output.setNoOfMissApp(values[i++]);
			    	output.setNoOfComplMeas(values[i++]);
			    	output.setHistoryOfNonCom(values[i++]);
			    	output.setHypertension(values[i++]);
			    	output.setDiabets(values[i++]);
			    	output.setSmokeYarn(values[i++]);
			    	output.setAlcoholYarn(values[i++]);
			    	output.setMentalhealYarn(values[i++]);
			    	output.setNoOfIpVisit(values[i++]);
			    	output.setNoOfOpVisit(values[i++]);
			    	output.setNoOfErVisit(values[i++]);
			    	output.setDaySpending(values[i++]);
			    	output.setPlanCoverRatio(values[i++]);
			    	output.setCompiancePotential(values[i++]);
			    	output.setPredictPotential(values[i++]);
				    setOutput.add(output);
		    	}
		    	L++;
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
	public Set<ModelSummary> getComplianceModelSummary() {
		
		Set<ModelSummary> setOutput = new HashSet<>();
	    BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(windowsCopyPath+"/compliance/ModelSummary.csv"));
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
			    	output.setStdError(values[2]);
			    	output.setzValue(values[3]);
			    	output.setPrz(values[4]);			    	
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
	public ModelMetric getComplianceModelMetric() {
		ModelMetric modelMetric = new ModelMetric();
	    BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(windowsCopyPath+"/compliance/ModelMetric.csv"));
		    String line = null;
		    int i = 0;
		    while ((line = br.readLine()) != null) {
		    	i++;
		    	if(i == 1) continue;
		    	String[] values = line.split(",");
		    	if(values.length > 2) {
		    		modelMetric.setTp(values[0]);
		    		modelMetric.setFp(values[1]);
		    		modelMetric.setTn(values[2]);
		    		modelMetric.setFn(values[3]);
		    		modelMetric.setScore(values[4]);
		    		modelMetric.setImagePath(windowsCopyPath+"/compliance/ROCplot.jpeg");
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
		
		return modelMetric;
	}

	

}
