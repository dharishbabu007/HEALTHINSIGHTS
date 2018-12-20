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
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.ClusterAnalysis;
import com.qms.rest.model.ClusterCateVar;
import com.qms.rest.model.ClusterContVar;
import com.qms.rest.model.ClusterData;
import com.qms.rest.model.ClusterPersona;
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.LHEOutput;
import com.qms.rest.model.ModelMetric;
import com.qms.rest.model.ModelScore;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.RestResult;
import com.qms.rest.util.QMSAnalyticsProperty;
import com.qms.rest.util.QMSConnection;

@Service("memberEngagementService")
public class MemberEngagementServiceImpl implements MemberEngagementService {
	
	@Autowired
	private QMSAnalyticsProperty qmsAnalyticsProperty;
	
	String windowsCopyPath;
	
	@Autowired
	private QMSConnection qmsConnection;	
	
	@PostConstruct
    public void init() {
		windowsCopyPath = qmsAnalyticsProperty.getWindowsCopyPath();
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
	public Set<ClusterAnalysis> getCSVClusterAnalysis() {
		Set<ClusterAnalysis> setOutput = new HashSet<>();
		
	    BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(windowsCopyPath+"/ClusteringStatistics.csv"));
		    String line = null;
		    ClusterAnalysis output = null;
		    int i = 0;
		    while ((line = br.readLine()) != null) {
		    	i++;
		    	if(i == 1) continue;
		    	String[] values = line.split(",");
		    	if(values.length > 6) {
			    	output = new ClusterAnalysis();
			    	output.setAggStage(values[0]);
			    	output.setTest1(values[1]);
			    	output.setTest2(values[2]);
			    	output.setTest3(values[3]);
			    	output.setTest4(values[4]);
			    	output.setTest5(values[5]);
			    	output.setTest6(values[6]);			    	
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
		//return transData;
	}

	@Override
	public RestResult updateClusteringPersona(ClusterPersona clusteringPersona) {
		
		String sqlStatementInsert = "insert into ME_CLUS_PERSONA (CLUSTER_ID,BARRIERS,DEMOGRAPHICS,GOALS,"
				+ "HEALTH_STATUS,MOTIVATIONS,PERSONA_NAME,SOCIAL_MEDIA) "
				+ "values (?,?,?,?,?,?,?,?)";
		
		PreparedStatement statement = null;
		Connection connection = null;
		Statement statementObj = null;
		try {	
			connection = qmsConnection.getOracleConnection();
			
			statementObj = connection.createStatement();				
			statementObj.executeUpdate("delete from ME_CLUS_PERSONA where CLUSTER_ID="+clusteringPersona.getClusterId());
			
			statement = connection.prepareStatement(sqlStatementInsert);			
			int i=0;							
			statement.setInt(++i, clusteringPersona.getClusterId());
			statement.setString(++i, clusteringPersona.getBarriers());
			statement.setString(++i, clusteringPersona.getDemographics());
			statement.setString(++i, clusteringPersona.getGoals());
			statement.setString(++i, clusteringPersona.getHealthStatus());
			statement.setString(++i, clusteringPersona.getMotivations());
			statement.setString(++i, clusteringPersona.getPersonaName());
			statement.setString(++i, clusteringPersona.getSocialMedia());
			statement.executeUpdate();
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
	public ClusterData getClusteringData(int clusterId) {
		
		ClusterData clusterData = new ClusterData();
		Set<ClusterCateVar> clusterCateVars = new HashSet<>();
		Set<ClusterContVar> clusterContVars = new HashSet<>();
		ClusterPersona clusterPersona = new ClusterPersona();
		
		clusterData.setClusterCateVars(clusterCateVars);
		clusterData.setClusterContVars(clusterContVars);
		clusterData.setClusterPersona(clusterPersona);
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from ME_CLUS_CATE_VAR where CLUSTER_ID="+clusterId);			
			ClusterCateVar clusterCateVar = null;
			while (resultSet.next()) {
				clusterCateVar = new ClusterCateVar();
				clusterCateVar.setAttribute(resultSet.getString("ATTRIBUTE"));
				clusterCateVar.setClusterId(resultSet.getString("CLUSTER_ID"));
				clusterCateVar.setLabel(resultSet.getString("LABEL"));
				clusterCateVar.setValue(resultSet.getString("VALUE"));
		    	clusterCateVars.add(clusterCateVar);
			}
			
			resultSet.close();			
			resultSet = statement.executeQuery("select * from ME_CLUS_CONT_VAR where CLUSTER_ID="+clusterId);			
			ClusterContVar clusterContVar = null;
			while (resultSet.next()) {
				clusterContVar = new ClusterContVar();			  
				clusterContVar.setAttribute(resultSet.getString("ATTRIBUTE"));
				clusterContVar.setClusterId(resultSet.getString("CLUSTER_ID"));
				clusterContVar.setFirstQuartile(resultSet.getString("FIRST_QUARTILE"));
				clusterContVar.setMedian(resultSet.getString("MEDIAN"));
				clusterContVar.setMax(resultSet.getString("MAX"));
				clusterContVar.setMin(resultSet.getString("MIN"));
				clusterContVar.setSecondQuartile(resultSet.getString("SECOND_QUARTILE"));
				clusterContVars.add(clusterContVar);
			}
			
			resultSet.close();			
			resultSet = statement.executeQuery("select * from ME_CLUS_PERSONA where CLUSTER_ID="+clusterId);			
			while (resultSet.next()) {
				clusterPersona.setBarriers(resultSet.getString("BARRIERS"));
				clusterPersona.setClusterId(resultSet.getInt("CLUSTER_ID"));
				clusterPersona.setDemographics(resultSet.getString("DEMOGRAPHICS"));
				clusterPersona.setGoals(resultSet.getString("GOALS"));
				clusterPersona.setHealthStatus(resultSet.getString("HEALTH_STATUS"));
				clusterPersona.setMotivations(resultSet.getString("MOTIVATIONS"));
				clusterPersona.setPersonaName(resultSet.getString("PERSONA_NAME"));
				clusterPersona.setSocialMedia(resultSet.getString("SOCIAL_MEDIA"));
								
			}						
			
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		
		return clusterData;
	}

	@Override
	public Set<LHEOutput> getLHEModelOutPut() {
		Set<LHEOutput> setOutput = new HashSet<>();
			
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("SELECT LFO.*, DM.first_name, DM.middle_name, DM.last_name from analytics.LHE_FILE_OUTPUT LFO "
					+ "LEFT OUTER JOIN  healthin.DIM_MEMBER DM ON (LFO.MEMBER_ID=DM.MEMBER_ID)");
			//resultSet = statement.executeQuery("select * from ns_file_output where fid='45' limit 500");
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
			    setOutput.add(output);
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		return setOutput;
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

		int columns = 6;
		int rows = 0;
		String[][] transData = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select count(*) from LHE_CLUSTERING_STATISTICS");
			while (resultSet.next()) {
				rows = resultSet.getInt(1);
				System.out.println(" total rows in LHE_CLUSTERING_STATISTICS --> " + rows);
			}			
			rows = rows+1;
			resultSet.close();
			resultSet = statement.executeQuery("select * from LHE_CLUSTERING_STATISTICS");
			String[][] data = new String [rows][columns]; 
			data[0][0] = "agglomeration_stage";
			data[0][1] = "within_cluster_ss";
			data[0][2] = "average_within";
			data[0][3] = "average_between";
			data[0][4] = "average_silwidth";
			data[0][5] = "modelid";
			int i = 1;
			int j = 0;
			while (resultSet.next()) {
				j = 0;
				data[i][j++] = resultSet.getString("agglomeration_stage");
				data[i][j++] = resultSet.getString("within_cluster_ss");
				data[i][j++] = resultSet.getString("average_within");
				data[i][j++] = resultSet.getString("average_between");
				data[i][j++] = resultSet.getString("average_silwidth");
				data[i][j++] = resultSet.getString("modelid");
				i++;
			}
			
//			for (i = 0; i < rows; i++) {
//				for (j = 0; j < columns; j++) {
//					System.out.print(data[i][j] + " ");
//				}
//				System.out.println();
//			}
			
			//transpose
			transData = new String[columns][rows];
			for (i = 0; i < rows; i++) {
				for (j = 0; j < columns; j++) {
					transData[j][i] = data[i][j];
				}
			}
			
//			System.out.println(" after transpose.... ");
//			for (i = 0; i < columns; i++) {
//				for (j = 0; j < rows; j++) {
//					System.out.print(transData[i][j] + " ");
//				}
//				System.out.println();
//			}
			
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		return transData;
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
			String csvFilePath = windowsCopyPath+"/LHE_FILE_INPUT/LHE_FILE_INPUT_"+System.currentTimeMillis()+".csv";
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
			return RestResult.getSucessRestResult("Input file creation sucess.");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
	}
	
	
	
	
}
