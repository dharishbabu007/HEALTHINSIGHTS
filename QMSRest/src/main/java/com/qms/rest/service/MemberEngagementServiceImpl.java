package com.qms.rest.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.CloseGap;
import com.qms.rest.model.ClusterAnalysis;
import com.qms.rest.model.ClusterCateVar;
import com.qms.rest.model.ClusterContVar;
import com.qms.rest.model.ClusterData;
import com.qms.rest.model.ClusterPersona;
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.ModelScore;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.RestResult;
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

}
