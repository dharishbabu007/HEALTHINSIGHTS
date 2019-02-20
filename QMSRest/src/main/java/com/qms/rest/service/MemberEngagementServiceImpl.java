package com.qms.rest.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.qms.rest.model.ClusterAnalysis;
import com.qms.rest.model.ClusterCateVar;
import com.qms.rest.model.ClusterContVar;
import com.qms.rest.model.ClusterData;
import com.qms.rest.model.PersonaDefine;
import com.qms.rest.model.PersonaMember;
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.GraphData;
import com.qms.rest.model.LHCMember;
import com.qms.rest.model.LHEOutput;
import com.qms.rest.model.ModelMetric;
import com.qms.rest.model.ModelScore;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.PersonaClusterFeatures;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RoleLandingPage;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSAnalyticsProperty;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;

@Service("memberEngagementService")
public class MemberEngagementServiceImpl implements MemberEngagementService {
	
	@Autowired
	private QMSAnalyticsProperty qmsAnalyticsProperty;
	
	String windowsCopyPath;
	
	@Autowired 
	private HttpSession httpSession;	
	
	@Autowired
	private QMSConnection qmsConnection;	
	
	Set<LHEOutput> lheModelOutPut = new HashSet<>();
	
	@PostConstruct
    public void init() {
		windowsCopyPath = qmsAnalyticsProperty.getWindowsCopyPath();
		//getLHEModelOutPut();
    }	

	@Override
	public Set<ModelSummary> getCSVModelSummary() {
		Set<ModelSummary> setOutput = new HashSet<>();
		
	    BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(windowsCopyPath+"/ME_ModelSummary_v2.csv"));
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

	@Override
	public String[][] getCSVClusterAnalysis() {
		//Set<ClusterAnalysis>
		String[] columnNames = {"cluster_id", "size", "withinss", "totwithinss", "betweenss", "totss"};
		int columns = columnNames.length;
		int rows = 0;
		
		String[][] transData = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			
			List<String[]> dataList = new ArrayList<>();
			resultSet = statement.executeQuery("select * from cp_statistics");
			String[] dataAry = null;
			while (resultSet.next()) {
				dataAry = new String[columns];
				for(int c=0; c < columnNames.length; c++) {
					dataAry[c] = resultSet.getString(columnNames[c]);
				}
				dataList.add(dataAry);					
			}			
			rows = dataList.size()+1;
			
			String[][] data = new String [rows][columns];
			for(int c=0; c < columnNames.length; c++)
				data[0][c] = columnNames[c];
					
			int i = 1;
			int j = 0;
			for(int l=0; l < dataList.size(); l++) {
				j = 0;
				dataAry = dataList.get(l);
				for(int c=0; c < dataAry.length; c++)
					data[i][j++] = dataAry[c];				
				i++;				
			}
			
			//transpose
			transData = new String[columns][rows];
			for (i = 0; i < rows; i++) {
				for (j = 0; j < columns; j++) {
					transData[j][i] = data[i][j];
				}
			}
						
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		return transData;		
		
		
//		Set<ClusterAnalysis> setOutput = new HashSet<>();		
//	    BufferedReader br = null;
//		try {
//			br = new BufferedReader(new FileReader(windowsCopyPath+"/ClusteringStatistics.csv"));
//		    String line = null;
//		    ClusterAnalysis output = null;
//		    int i = 0;
//		    while ((line = br.readLine()) != null) {
//		    	i++;
//		    	if(i == 1) continue;
//		    	String[] values = line.split(",");
//		    	if(values.length > 6) {
//			    	output = new ClusterAnalysis();
//			    	output.setAggStage(values[0]);
//			    	output.setTest1(values[1]);
//			    	output.setTest2(values[2]);
//			    	output.setTest3(values[3]);
//			    	output.setTest4(values[4]);
//			    	output.setTest5(values[5]);
//			    	output.setTest6(values[6]);			    	
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
//		return setOutput;
	}

	@Override
	public RestResult updateClusteringPersona(PersonaDefine clusteringPersona) {

		//PERSONANAME,PERSONACREATEDATE,DEMO_AGEGROUP,DEMO_EDUCATION,DEMO_INCOME,DEMO_OCCUPATION,DEMO_ADDICTIONS,DEMO_FAMILYSIZE,MOTIVATIONS,GOALS,BARRIERS,PERSON_SOCIAL_MEDIA,HEALTH_STATUS,IMAGE_URL,BIO
//		String sqlStatementInsert = "insert into ME_CLUS_PERSONA (CLUSTER_ID,BARRIERS,DEMOGRAPHICS,GOALS,"
//				+ "HEALTH_STATUS,MOTIVATIONS,PERSONA_NAME,SOCIAL_MEDIA) "
//				+ "values (?,?,?,?,?,?,?,?)";
		String sqlStatementInsert = "insert into QMS_CP_Define (CLUSTER_ID,PERSONA_NAME,PERSONA_CREATE_DATE,"
				+ "DEMO_AGEGROUP,DEMO_EDUCATION,DEMO_INCOME,DEMO_OCCUPATION,DEMO_ADDICTIONS,"
				+ "DEMO_FAMILYSIZE,MOTIVATIONS,GOALS,BARRIERS,PERSON_SOCIAL_MEDIA,HEALTH_STATUS,"
				+ "IMAGE_URL,BIO,"
				+ "curr_flag,rec_create_date,rec_update_date,latest_flag,active_flag,ingestion_date,source_name,user_name"
				+ ") "
				+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				//+ "values (?,?,CURRENT_DATE(),?,?,?,?,?,?,?,?,?,?,?,?,?)";		
				//+ "curr_flag,rec_create_date,rec_update_date,latest_flag,active_flag,ingestion_date,source_name,"
		PreparedStatement statement = null;
		Connection connection = null;
		Statement statementObj = null;
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		try {	
			connection = qmsConnection.getOracleConnection();
			
			statementObj = connection.createStatement();				
			statementObj.executeUpdate("delete from QMS_CP_Define where CLUSTER_ID="+clusteringPersona.getClusterId());
			
			statement = connection.prepareStatement(sqlStatementInsert);			
			int i=0;							
			statement.setInt(++i, clusteringPersona.getClusterId());			
			statement.setString(++i, clusteringPersona.getPersonaName());	
			
			Date date = new Date();				
			Timestamp timestamp = new Timestamp(date.getTime());	
			statement.setTimestamp(++i, timestamp);			
			
			statement.setString(++i, clusteringPersona.getDemoAgeGroup());
			statement.setString(++i, clusteringPersona.getDemoEducation());
			statement.setString(++i, clusteringPersona.getDemoIncome());
			statement.setString(++i, clusteringPersona.getDemoOccupation());
			statement.setString(++i, clusteringPersona.getDemoAddictions());
			statement.setString(++i, clusteringPersona.getDemoFamilySize());
			statement.setString(++i, clusteringPersona.getMotivations());
			statement.setString(++i, clusteringPersona.getGoals());
			statement.setString(++i, clusteringPersona.getBarriers());		
			statement.setString(++i, clusteringPersona.getSocialMedia());
			statement.setString(++i, clusteringPersona.getHealthStatus());
			statement.setString(++i, clusteringPersona.getImageUrl());
			statement.setString(++i, clusteringPersona.getBio());
			
			statement.setString(++i, "Y");
			statement.setTimestamp(++i, timestamp);
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i, "Y");
			statement.setString(++i, "A");
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i, "UI");				
			
			if(userData != null && userData.getName() != null)
				statement.setString(++i, userData.getName());
			else 
				statement.setString(++i, QMSConstants.MEASURE_USER_NAME);			
			
			
//			statement.setInt(++i, clusteringPersona.getClusterId());
//			statement.setString(++i, clusteringPersona.getBarriers());
//			statement.setString(++i, clusteringPersona.getDemographics());
//			statement.setString(++i, clusteringPersona.getGoals());
//			statement.setString(++i, clusteringPersona.getHealthStatus());
//			statement.setString(++i, clusteringPersona.getMotivations());
//			statement.setString(++i, clusteringPersona.getPersonaName());
//			statement.setString(++i, clusteringPersona.getSocialMedia());
			statement.executeUpdate();
			//connection.commit();
			return RestResult.getSucessRestResult(" Cluster Persona update Success. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(null, statementObj, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}		
	}
	
	
	@Override
	public ClusterData getClusteringData(String clusterId) {
		
		ClusterData clusterData = new ClusterData();
		PersonaDefine clusterPersona = new PersonaDefine();
		PersonaClusterFeatures personaClusterFeatures = new PersonaClusterFeatures();
		clusterData.setPersonaClusterFeatures(personaClusterFeatures);
		clusterData.setClusterPersona(clusterPersona);
		
		ResultSet resultSet = null;		
		Statement phoenixStatement = null;
		Connection phoenixConnection = null;		
		try {						
			phoenixConnection = qmsConnection.getOracleConnection();
			phoenixStatement = phoenixConnection.createStatement();
			resultSet = phoenixStatement.executeQuery("select * from QMS_CP_Define where CLUSTER_ID="+clusterId);
			String personaName = null;
			while (resultSet.next()) {
				clusterPersona.setBarriers(resultSet.getString("BARRIERS"));
				clusterPersona.setClusterId(resultSet.getInt("CLUSTER_ID"));
				clusterPersona.setGoals(resultSet.getString("GOALS"));
				clusterPersona.setHealthStatus(resultSet.getString("HEALTH_STATUS"));
				clusterPersona.setMotivations(resultSet.getString("MOTIVATIONS"));
				personaName = resultSet.getString("PERSONA_NAME");
				clusterPersona.setPersonaName(personaName);
				clusterPersona.setSocialMedia(resultSet.getString("PERSON_SOCIAL_MEDIA"));
				
				clusterPersona.setDemoAgeGroup(resultSet.getString("DEMO_AGEGROUP"));
				clusterPersona.setDemoEducation(resultSet.getString("DEMO_EDUCATION"));
				clusterPersona.setDemoIncome(resultSet.getString("DEMO_INCOME"));
				clusterPersona.setDemoOccupation(resultSet.getString("DEMO_OCCUPATION"));
				clusterPersona.setDemoAddictions(resultSet.getString("DEMO_ADDICTIONS"));
				clusterPersona.setDemoFamilySize(resultSet.getString("DEMO_FAMILYSIZE"));
				clusterPersona.setImageUrl(resultSet.getString("IMAGE_URL"));
				clusterPersona.setBio(resultSet.getString("BIO"));
			}
			
			resultSet.close();
			resultSet = phoenixStatement.executeQuery("select * from QMS_CP_Features where cluster_id='"+clusterId+"'");
			while (resultSet.next()) {
				personaClusterFeatures.setPersonaName(personaName);
				personaClusterFeatures.setClusterId(resultSet.getString("cluster_id"));
				personaClusterFeatures.setFeatureName(resultSet.getString("FEATURE_NAME"));
				personaClusterFeatures.setFeatureType(resultSet.getString("FEATURE_TYPE"));
				personaClusterFeatures.setFeatureSignificanceValue(resultSet.getString("FEATURE_SIGNIFICANCE"));
				personaClusterFeatures.setMaxFrequency(resultSet.getString("MAX_FREQUENCY"));
				personaClusterFeatures.setX(resultSet.getString("X"));
				personaClusterFeatures.setY(resultSet.getInt("Y"));
			}			
			
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, phoenixStatement, phoenixConnection);
		}			
		
		return clusterData;
	}

	@Override
	public Set<LHEOutput> getLHEModelOutPut() {
		//Set<LHEOutput> lheModelOutPut = new HashSet<>();
		
		if(lheModelOutPut != null && !lheModelOutPut.isEmpty()) {
			System.out.println(" Getting LHE Output data from cache ");
			return lheModelOutPut;
		} else {
			System.out.println(" Loading LHE Output data ");
			Statement statement = null;
			ResultSet resultSet = null;		
			Connection connection = null;
			try {						
				connection = qmsConnection.getHiveConnection();
				statement = connection.createStatement();			
	//			resultSet = statement.executeQuery("SELECT LFO.*, DM.first_name, DM.middle_name, DM.last_name from analytics.LHE_FILE_OUTPUT LFO "
	//					+ "LEFT OUTER JOIN  healthin.DIM_MEMBER DM ON (LFO.MEMBER_ID=DM.MEMBER_ID)");
				resultSet = statement.executeQuery("SELECT LFO.MEMBER_ID,LFO.ENROLLMENT_GAPS,LFO.OUT_OF_POCKET_EXPENSES,"
						+ "LFO.UTILIZER_CATEGORY,LFO.AGE,LFO.AMOUNT_SPEND,LFO.ER,LFO.REASON_TO_NOT_ENROLL,LFO.likelihood_enrollment,"
						+ "LFO.ENROLLMENT_BIN, DM.first_name, DM.middle_name, DM.last_name from analytics.LHE_FILE_OUTPUT LFO "
						+ "LEFT OUTER JOIN  healthin.DIM_MEMBER DM ON (LFO.MEMBER_ID=DM.MEMBER_ID)");
				LHEOutput output = null;
				String name = "";			
				while (resultSet.next()) {
			    	output = new LHEOutput();			    
					output.setMemberId(resultSet.getString("MEMBER_ID"));
					name = "";
					if(resultSet.getString("first_name") != null)
						name = resultSet.getString("first_name");
					if(resultSet.getString("middle_name") != null)
						name = name+" "+resultSet.getString("middle_name");
					if(resultSet.getString("last_name") != null)				
						name = name+" "+resultSet.getString("last_name");
					output.setMemberName(name);
					output.setEnrollGaps(resultSet.getString("ENROLLMENT_GAPS"));
					output.setOutOfPocketExpenses(resultSet.getString("OUT_OF_POCKET_EXPENSES"));
					output.setUtilizerCategory(resultSet.getString("UTILIZER_CATEGORY"));
					output.setAge(resultSet.getString("AGE"));
					output.setAmountSpend(resultSet.getString("AMOUNT_SPEND"));
					output.setEr(resultSet.getString("ER"));
					output.setReasonNotEnroll(resultSet.getString("REASON_TO_NOT_ENROLL"));
					output.setLikeliHoodEnroll(resultSet.getString("likelihood_enrollment"));
					output.setEnrollmentBin(resultSet.getString("ENROLLMENT_BIN"));
					lheModelOutPut.add(output);
				}
			} catch (Exception e) {
				e.printStackTrace();			
			}
			finally {
				qmsConnection.closeJDBCResources(resultSet, statement, connection);
			}		
		}
		return lheModelOutPut;
	}

	@Override
	public Set<ModelSummary> getLHEModelSummary() {
		Set<ModelSummary> setOutput = new HashSet<>();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			//resultSet = statement.executeQuery("select * from LHE_MODEL_SUMMARY where modelid='1'");			
			resultSet = statement.executeQuery("select * from LHE_MODEL_SUMMARY");
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
		
		return setOutput;
	}

	@Override
	public ModelMetric getLHEModelMetric() {
		ModelMetric modelMetric = new ModelMetric();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from LHE_MODEL_METRIC");			
			while (resultSet.next()) {
	    		modelMetric.setTp(resultSet.getString("TP"));
	    		modelMetric.setFp(resultSet.getString("FP"));
	    		modelMetric.setTn(resultSet.getString("TN"));
	    		modelMetric.setFn(resultSet.getString("FN"));
	    		modelMetric.setScore(resultSet.getString("SCORE"));
	    		modelMetric.setImagePath(windowsCopyPath+"/ROCplot.PNG");
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		
		return modelMetric;
	}

	@Override
	public String[][] getLHEReasonNotEnrollStatics() {
		
		String[] columnNames = {"cluster_id", "size", "withinss", "totwithinss", "betweenss", "totss"};
		int columns = columnNames.length;
		int rows = 0;
		
		String[][] transData = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			
			List<String[]> dataList = new ArrayList<>();
			resultSet = statement.executeQuery("select * from LHE_CLUSTERING_STATISTICS");
			String[] dataAry = null;
			while (resultSet.next()) {
				dataAry = new String[columns];
				for(int c=0; c < columnNames.length; c++) {
					dataAry[c] = resultSet.getString(columnNames[c]);
				}
				dataList.add(dataAry);					
			}			
			rows = dataList.size()+1;
			
			String[][] data = new String [rows][columns];
			for(int c=0; c < columnNames.length; c++)
				data[0][c] = columnNames[c];
					
			int i = 1;
			int j = 0;
			for(int l=0; l < dataList.size(); l++) {
				j = 0;
				dataAry = dataList.get(l);
				for(int c=0; c < dataAry.length; c++)
					data[i][j++] = dataAry[c];				
				i++;				
			}
			
			//transpose
			transData = new String[columns][rows];
			for (i = 0; i < rows; i++) {
				for (j = 0; j < columns; j++) {
					transData[j][i] = data[i][j];
				}
			}
						
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		return transData;		
		

//		int columns = 6;
//		int rows = 0;
//		String[][] transData = null;
//		Statement statement = null;
//		ResultSet resultSet = null;		
//		Connection connection = null;
//		try {
//			connection = qmsConnection.getHiveConnection();
//			statement = connection.createStatement();			
//			resultSet = statement.executeQuery("select count(*) from LHE_CLUSTERING_STATISTICS");
//			while (resultSet.next()) {
//				rows = resultSet.getInt(1);
//				System.out.println(" total rows in LHE_CLUSTERING_STATISTICS --> " + rows);
//			}			
//			rows = rows+1;
//			resultSet.close();
//			resultSet = statement.executeQuery("select * from LHE_CLUSTERING_STATISTICS");
//			String[][] data = new String [rows][columns]; 
//			data[0][0] = "agglomeration_stage";
//			data[0][1] = "within_cluster_ss";
//			data[0][2] = "average_within";
//			data[0][3] = "average_between";
//			data[0][4] = "average_silwidth";
//			data[0][5] = "modelid";
//			int i = 1;
//			int j = 0;
//			while (resultSet.next()) {
//				j = 0;
//				data[i][j++] = resultSet.getString("agglomeration_stage");
//				data[i][j++] = resultSet.getString("within_cluster_ss");
//				data[i][j++] = resultSet.getString("average_within");
//				data[i][j++] = resultSet.getString("average_between");
//				data[i][j++] = resultSet.getString("average_silwidth");
//				data[i][j++] = resultSet.getString("modelid");
//				i++;
//			}
//			
////			for (i = 0; i < rows; i++) {
////				for (j = 0; j < columns; j++) {
////					System.out.print(data[i][j] + " ");
////				}
////				System.out.println();
////			}
//			
//			//transpose
//			transData = new String[columns][rows];
//			for (i = 0; i < rows; i++) {
//				for (j = 0; j < columns; j++) {
//					transData[j][i] = data[i][j];
//				}
//			}
//			
////			System.out.println(" after transpose.... ");
////			for (i = 0; i < columns; i++) {
////				for (j = 0; j < rows; j++) {
////					System.out.print(transData[i][j] + " ");
////				}
////				System.out.println();
////			}
//			
//		} catch (Exception e) {
//			e.printStackTrace();			
//		}
//		finally {
//			qmsConnection.closeJDBCResources(resultSet, statement, connection);
//		}		
//		
//		return transData;
	}

	@Override
	public RestResult createLHEInputFile() {
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {
			
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from LHE_FILE_INPUT");
			
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			String[] columns = new String[columnCount]; 
			for(int i = 1; i <= columnCount; i++) {
				columns[i-1] = metaData.getColumnName(i).replaceAll("lhe_file_input.", "");
				System.out.println(columns[i-1]);
			}
			
			//headers
			String headers = "";
			for(int i = 0; i < columns.length; i++) {
				if(i != (columns.length-1)) {
					headers = headers+columns[i] + ",";
				} else {
					headers = headers+columns[i];
				}
			}
			
			//file creation
			//String csvFilePath = windowsCopyPath+"/LHE_FILE_INPUT/LHE_FILE_INPUT_"+System.currentTimeMillis()+".csv";
			String csvFilePath = windowsCopyPath+"/LHE_FILE_INPUT/LHE_FILE_INPUT.csv";
			FileWriter fw = new FileWriter(csvFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(headers+"\n");
			
			//values
			String values = "";
			while (resultSet.next()) {
				values = "";
				for(int i = 0; i < columns.length; i++) {
					if(i != (columns.length-1)) {
						values = values + resultSet.getString(columns[i]) + ",";
					} else {
						values = values + resultSet.getString(columns[i]);
					}
				}	
				bw.write(values+"\n");
			}
			
			bw.close();
			Thread.sleep(2000);
			
			//call R API
			RestTemplate restTemplate = new RestTemplate();		
			String result = restTemplate.getForObject("http://localhost:8081/lhe", String.class);
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	        System.out.println(" R API Rest Result --> " + result);			
			
			
			return RestResult.getSucessRestResult("Input file creation sucess.");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
	}

	@Override
	public GraphData getPersonaClusterFeaturesGraphData(String clusterId, String attributeName) {
		GraphData graphData = new GraphData();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {
			List<String> xData = new ArrayList<>();
			List<Integer> yData = new ArrayList<>();
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select x,y from cp_features where cluster_id='"+clusterId+"' and feature_name='"+attributeName+"'");			
			while (resultSet.next()) {
				xData.add(resultSet.getString("x"));
				yData.add(resultSet.getInt("y"));
			}
			graphData.setX(xData);
			graphData.setY(yData);
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		return graphData;
	}

	@Override
	public Set<PersonaClusterFeatures> getPersonaClusterFeatures(String clusterId) {
		Set<PersonaClusterFeatures> personaClusterFeaturesList = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from cp_features where cluster_id='"+clusterId+"'");
			PersonaClusterFeatures personaClusterFeatures = null;
			while (resultSet.next()) {
				personaClusterFeatures = new PersonaClusterFeatures();
				personaClusterFeatures.setClusterId(resultSet.getString("cluster_id"));
				personaClusterFeatures.setFeatureName(resultSet.getString("feature_name"));
				personaClusterFeatures.setFeatureType(resultSet.getString("feature_type"));
				if(resultSet.getString("feature_significance_value") != null)
					personaClusterFeatures.setFeatureSignificanceValue(resultSet.getString("feature_significance_value"));
				if(resultSet.getString("max_frequency") != null)
					personaClusterFeatures.setMaxFrequency(resultSet.getString("max_frequency"));
				//personaClusterFeatures.setModelId(resultSet.getString("modelid"));				
				personaClusterFeaturesList.add(personaClusterFeatures);
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return personaClusterFeaturesList;
	}

	@Override
	public Set<String> getPersonaClusterNames() {
		Set<String> namesSet = new TreeSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			//connection = qmsConnection.getPhoenixConnection();
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			//resultSet = statement.executeQuery("select distinct CLUSTER_ID from QMS.qms_cp_file_output");			
			resultSet = statement.executeQuery("select distinct CLUSTER_ID from qms_cp_file_output");
			while (resultSet.next()) {
				namesSet.add(resultSet.getString("CLUSTER_ID"));
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		
		return namesSet;		
	}
	
	
	@Override
	public RestResult createPersonaInputFile() {
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {
			
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from Persona_File_Input");
			
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			String[] columns = new String[columnCount]; 
			for(int i = 1; i <= columnCount; i++) {
				columns[i-1] = metaData.getColumnName(i).replaceAll("persona_file_input.", "");
				System.out.println(columns[i-1]);
			}
			
			//headers
			String headers = "";
			for(int i = 0; i < columns.length; i++) {
				if(i != (columns.length-1)) {
					headers = headers+columns[i] + ",";
				} else {
					headers = headers+columns[i];
				}
			}
			
			//file creation
			//String csvFilePath = windowsCopyPath+"/LHE_FILE_INPUT/LHE_FILE_INPUT_"+System.currentTimeMillis()+".csv";
			String csvFilePath = windowsCopyPath+"/PERSONA_FILE_INPUT/PERSONA_FILE_INPUT.csv";
			FileWriter fw = new FileWriter(csvFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(headers+"\n");
			
			//values
			String values = "";
			while (resultSet.next()) {
				values = "";
				for(int i = 0; i < columns.length; i++) {
					if(i != (columns.length-1)) {
						values = values + resultSet.getString(columns[i]) + ",";
					} else {
						values = values + resultSet.getString(columns[i]);
					}
				}	
				bw.write(values+"\n");
			}
			
			bw.close();
			Thread.sleep(2000);
			
			//call R API
			RestTemplate restTemplate = new RestTemplate();		
			String result = restTemplate.getForObject("http://localhost:8081/persona", String.class);
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	        System.out.println(" R API Rest Result --> " + result);			
			
			
			return RestResult.getSucessRestResult("Input file creation sucess.");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
	}

	@Override
	public Set<PersonaMember> personaMemberList(String clusterId) {
		Set<PersonaMember> personaMembers = new HashSet<PersonaMember>();
		System.out.println(" Loading personaMemberList data ");
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getPhoenixConnection();
			statement = connection.createStatement();
			String qryStr = "select PD.*, PFO.* from QMS_CP_Define PD "+ 
					"INNER JOIN qms_cp_file_output PFO ON PFO.CLUSTER_ID = PD.CLUSTER_ID "+  
					"INNER JOIN Dim_Member DM ON DM.MEMBER_ID = PFO.MEMBER_ID "+  
					"where PD.CLUSTER_ID="+clusterId;
			resultSet = statement.executeQuery(qryStr);
			PersonaMember output = null;
			String name = "";			
			while (resultSet.next()) {
		    	output = new PersonaMember();			    
				name = "";
				if(resultSet.getString("first_name") != null)
					name = resultSet.getString("first_name");
				if(resultSet.getString("middle_name") != null)
					name = name+" "+resultSet.getString("middle_name");
				if(resultSet.getString("last_name") != null)				
					name = name+" "+resultSet.getString("last_name");
				output.setMemberName(name);
				
				//qms_cp_file_output
				output.setMemberID(resultSet.getString("MEMBER_ID"));
				output.setAge(resultSet.getString("AGE"));	
				output.setGender(resultSet.getString("GENDER"));	
				output.setFormExercise(resultSet.getString("FORM_OF_EXERCISE"));	
				output.setFrequencyExercise(resultSet.getString("FREQUENCY_OF_EXERCISE"));	
				output.setMotivation(resultSet.getString("MOTIVATIONS"));	
				output.setLikelihoodEnrollment(resultSet.getString("LIKELIHOOD_OF_ENROLLMENT"));	
				output.setReasonToNotEnroll(resultSet.getString("REASON_TO_NOT_ENROLL"));	
				output.setSetAchieveHealthGo(resultSet.getString("SET_AND_ACHIEVE_HEALTH_GOAL"));	
				output.setFamilySize(resultSet.getString("FAMILY_SIZE"));	
				output.setClusterID(resultSet.getString("CLUSTER_ID"));
				
				//Persona_Define
				output.setPersonaName(resultSet.getString("PERSONA_NAME"));				
				
				personaMembers.add(output);
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}	
		return personaMembers;
	}

	@Override
	public RestResult createLHCInputFile() {
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {
			
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from LHC_File_Input");
			
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			String[] columns = new String[columnCount]; 
			for(int i = 1; i <= columnCount; i++) {
				columns[i-1] = metaData.getColumnName(i).replaceAll("lhc_file_input.", "");
				System.out.println(columns[i-1]);
			}
			
			//headers
			String headers = "";
			for(int i = 0; i < columns.length; i++) {
				if(i != (columns.length-1)) {
					headers = headers+columns[i] + ",";
				} else {
					headers = headers+columns[i];
				}
			}
			
			//file creation
			//String csvFilePath = windowsCopyPath+"/LHE_FILE_INPUT/LHE_FILE_INPUT_"+System.currentTimeMillis()+".csv";
			String csvFilePath = windowsCopyPath+"/LHC_FILE_INPUT/LHC_FILE_INPUT.csv";
			FileWriter fw = new FileWriter(csvFilePath);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(headers+"\n");
			
			//values
			String values = "";
			while (resultSet.next()) {
				values = "";
				for(int i = 0; i < columns.length; i++) {
					if(i != (columns.length-1)) {
						values = values + resultSet.getString(columns[i]) + ",";
					} else {
						values = values + resultSet.getString(columns[i]);
					}
				}	
				bw.write(values+"\n");
			}
			
			bw.close();
			Thread.sleep(2000);
			
			//call R API
			RestTemplate restTemplate = new RestTemplate();		
			String result = restTemplate.getForObject("http://localhost:8081/lhc", String.class);
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	        System.out.println(" R API Rest Result --> " + result);			
			
			
			return RestResult.getSucessRestResult("Input file creation sucess.");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
	}

	@Override
	public Set<LHCMember> lhcMemberList() {
		Set<LHCMember> personaMembers = new HashSet<LHCMember>();
		System.out.println(" Loading lhcMemberList data ");
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {		
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();
			String qryStr = "select DM.*, LFO.* from LHC_File_Output LFO "+ 
					"INNER JOIN healthin.Dim_Member DM ON DM.MEMBER_ID = LFO.MEMBER_ID";
			resultSet = statement.executeQuery(qryStr);
			LHCMember output = null;
			String name = "";			
			while (resultSet.next()) {
		    	output = new LHCMember();			    
				name = "";
				if(resultSet.getString("first_name") != null)
					name = resultSet.getString("first_name");
				if(resultSet.getString("middle_name") != null)
					name = name+" "+resultSet.getString("middle_name");
				if(resultSet.getString("last_name") != null)				
					name = name+" "+resultSet.getString("last_name");
				output.setMemberName(name);
				
				output.setMemberID(resultSet.getString("MEMBER_ID"));
				output.setAge(resultSet.getString("AGE"));	
				output.setGender(resultSet.getString("GENDER"));	
				output.setFrequencyExercise(resultSet.getString("FREQUENCY_OF_EXERCISE"));	
				output.setMotivations(resultSet.getString("MOTIVATIONS"));	
				output.setPersona(resultSet.getString("Persona"));
				output.setParticipationQuotient(resultSet.getString("Participation_quotient"));
				output.setComorbidityCount(resultSet.getString("Comorbidity_Count"));	
				output.setEnrollmentGaps(resultSet.getString("Enrollment_Gaps"));	
				output.setAmountSpend(resultSet.getString("Amount_Spend"));	
				output.setLikelihoodChurn(resultSet.getString("Likelihood_to_churn"));
				output.setChurn(resultSet.getString("predicted_churn"));				
				
				personaMembers.add(output);
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}	
		return personaMembers;
	}

	@Override
	public Set<ModelSummary> getLHCModelSummary() {
		Set<ModelSummary> setOutput = new HashSet<>();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			//resultSet = statement.executeQuery("select * from LHE_MODEL_SUMMARY where modelid='1'");			
			resultSet = statement.executeQuery("select * from LHC_MODEL_SUMMARY");
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
		
		return setOutput;
	}

	@Override
	public ModelMetric getLHCModelMetric() {
		ModelMetric modelMetric = new ModelMetric();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from LHC_MODEL_METRIC");			
			while (resultSet.next()) {
	    		modelMetric.setTp(resultSet.getString("TP"));
	    		modelMetric.setFp(resultSet.getString("FP"));
	    		modelMetric.setTn(resultSet.getString("TN"));
	    		modelMetric.setFn(resultSet.getString("FN"));
	    		modelMetric.setScore(resultSet.getString("SCORE"));
	    		modelMetric.setImagePath(windowsCopyPath+"/ROCplot.PNG");
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		
		return modelMetric;
	}

	@Override
	public Set<RoleLandingPage> getRoleLandingPage() {
		Set<RoleLandingPage> setOutput = new HashSet<>();
		User userData = qmsConnection.getLoggedInUser();
		if(userData == null) {
			System.out.println(" Role id is null.. ");
			return setOutput;
		}
		List<String> rolesList = Arrays.asList(new String[]{"9", "10", "11"});
		String roleId = userData.getRoleId();
		if(!rolesList.contains(roleId) && !userData.getLoginId().equalsIgnoreCase("demo_user")) {
			System.out.println(" Unauthorized role.. " +roleId);
			return setOutput;
		}
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {			
			//connection = qmsConnection.getHiveConnection();
			connection = qmsConnection.getHiveConnectionBySchemaName(QMSConnection.HIVE_HEALTHIN_SCHEMA);
			statement = connection.createStatement();
			if(userData.getLoginId().equalsIgnoreCase("demo_user"))
				resultSet = statement.executeQuery("select * from role_landing_page");
			else
				resultSet = statement.executeQuery("select * from role_landing_page where role_id="+roleId);
			RoleLandingPage output = null;
			while (resultSet.next()) {
		    	output = new RoleLandingPage();			    
		    	output.setDescription(resultSet.getString("description"));
		    	output.setTitle(resultSet.getString("titles"));
		    	output.setType(resultSet.getString("type"));
		    	output.setValue(resultSet.getString("value"));
		    	output.setRoleId(resultSet.getString("role_id"));
			    setOutput.add(output);
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		System.out.println(roleId + " RoleLandingPage records size --> " + setOutput.size());
		
		return setOutput;
	}	
	
}
