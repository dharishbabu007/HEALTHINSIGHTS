package com.qms.rest.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.EnrollmentFileOutput;
import com.qms.rest.model.FactGoalInterventions;
import com.qms.rest.model.FactGoalRecommendations;
import com.qms.rest.model.FileDownload;
import com.qms.rest.model.GoalRecommendationSet;
import com.qms.rest.model.GoalRecommendationSetData;
import com.qms.rest.model.NameValue;
import com.qms.rest.model.Objectives;
import com.qms.rest.model.PersonaMemberListView;
import com.qms.rest.model.RefCalorieIntake;
import com.qms.rest.model.RefPhysicalActivity;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RewardRecommendationSet;
import com.qms.rest.model.RewardsFileOutput;
import com.qms.rest.model.RewardsRecommendations;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;
import com.qms.rest.util.QMSDateUtil;

@Service("enrollmentService")
public class EnrollmentServiceImpl implements EnrollmentService {
	
	@Autowired
	private QMSConnection qmsConnection;
	
	@Autowired
	private HttpSession httpSession;	

	@Override
	public List<EnrollmentFileOutput> getEnrollmentFileOutput(String criteria) {
		List<EnrollmentFileOutput> setOutput = new ArrayList<>();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {
//			String whereClause = null;
//			if(criteria.equalsIgnoreCase("CRM")) {
//				whereClause = "CRMFLAG='Y'";
//			} else if(criteria.equalsIgnoreCase("VERIFY")) {
//				whereClause = "VERIFYFLAG='Y'";
//			} else {
//				whereClause = "CRMFLAG<>'Y' and VERIFYFLAG<>'Y'";
//			}
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			//resultSet = statement.executeQuery("select * from QMS_ENROLLMENT_FILE_OUTPUT");
			String query = "SELECT (A.FIRST_NAME||' '||A.MIDDLE_NAME||' '||A.LAST_NAME) AS NAME, B.* "
			+"FROM DIM_MEMBER A "
			+"INNER JOIN QMS_ENROLLMENT_FILE_OUTPUT B ON A.MEMBER_ID = B.MEMBER_ID";
			resultSet = statement.executeQuery(query);
			EnrollmentFileOutput output = null;
			String crmFlag = null;
			String verifyFlag = null;
			while (resultSet.next()) {
				output = new EnrollmentFileOutput();
		    	verifyFlag = resultSet.getString("VERIFYFLAG");
		    	crmFlag = resultSet.getString("CRMFLAG");		    	
		    	output.setMemberId(resultSet.getString("MEMBER_ID"));
		    	output.setMemberName(resultSet.getString("NAME"));		    	
		    	output.setChannel(resultSet.getString("CHANNEL"));
		    	output.setReward1(resultSet.getString("REWARD1"));
		    	output.setReward2(resultSet.getString("REWARD2"));
		    	output.setReward3(resultSet.getString("REWARD3"));
		    	output.setLikelihoodEnrollment(resultSet.getString("LIKELIHOOD_ENROLLMENT"));
		    	output.setReasonNotEnroll(resultSet.getString("REASON_TO_NOT_ENROLL"));
		    	output.setAge(resultSet.getString("AGE"));
		    	output.setGender(resultSet.getString("GENDER"));
		    	output.setMaritalStatus(resultSet.getString("MARITAL_STATUS"));
		    	output.setAmountSpend(resultSet.getString("AMOUNT_SPEND"));
		    	output.setUtilizerCategory(resultSet.getString("UTILIZER_CATEGORY"));
		    	output.setComorbidityCount(resultSet.getString("COMORBIDITY_COUNT"));
		    	output.setEnrollmentGaps(resultSet.getString("ENROLLMENT_GAPS"));
		    	output.setDaysPendingTermination(resultSet.getString("DAYS_PENDING_FOR_TERMINATION"));
		    	output.setRemarks(resultSet.getString("REMARKS"));
		    	output.setCrmFlag(crmFlag);
		    	output.setVerifyFlag(verifyFlag);
				if(criteria.equalsIgnoreCase("CRM") && crmFlag != null && crmFlag.equalsIgnoreCase("Y")) {
					setOutput.add(output);	
				} else if(criteria.equalsIgnoreCase("VERIFY") && 
						verifyFlag != null && verifyFlag.equalsIgnoreCase("Y")) {
					setOutput.add(output);
				} else if(criteria.equalsIgnoreCase("home") 
						&& (crmFlag == null    || crmFlag.equalsIgnoreCase("N")) 
						&& (verifyFlag == null || verifyFlag.equalsIgnoreCase("N"))) {
					setOutput.add(output);
				}		    	
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		System.out.println(criteria+" getEnrollmentFileOutput records size --> " + setOutput.size());
		
		return setOutput;
	}

	@Override
	public RestResult updateEnrollmentFileOutput(List<EnrollmentFileOutput> enrollmentFileOutputList, 
			String flag) {

//		String sqlStatementInsert = "UPSERT into QMS.ENROLLMENT_FILE_OUTPUT (REMARKS,VERIFYFLAG,CRMFLAG) "
//				+ "values (?,?,?) where MEMBER_ID=?";		
		String sqlStatementInsert = "UPDATE QMS_ENROLLMENT_FILE_OUTPUT SET REMARKS=?,VERIFYFLAG=?,CRMFLAG=? "
				+ "where MEMBER_ID=?";		
		
		String crmFlag = "";
		String verifyFlag = "";
		if(flag.equalsIgnoreCase("CRM")) {
			crmFlag = "Y";
			verifyFlag = "N";			
		} else if(flag.equalsIgnoreCase("VERIFY")) {
			crmFlag = "N";
			verifyFlag = "Y";			
		}		
		PreparedStatement statement = null;
		Connection connection = null;
		try {	
			connection = qmsConnection.getOracleConnection();
			statement = connection.prepareStatement(sqlStatementInsert);			
			int i=0;							
			for (EnrollmentFileOutput enrollmentFileOutput : enrollmentFileOutputList) {
				i=0;
				statement.setString(++i, enrollmentFileOutput.getRemarks());
				statement.setString(++i, verifyFlag);
				statement.setString(++i, crmFlag);
				statement.setString(++i, enrollmentFileOutput.getMemberId());
				statement.addBatch();
			}
			statement.executeBatch();
			//connection.commit();
			return RestResult.getSucessRestResult(" Cluster Persona update Success. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(null, statement, connection);
		}		
	}
	
	@Override
	public Objectives getObjectivesByTitle(String title) {
		Objectives objectivesList = new Objectives();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select TITLE,METRIC,METRIC,METRIC_VALUE,Y_O_Y,PERIOD,PERIOD_VALUE from QMS_OBJECTIVES where "
					+ "TITLE='" + title + "'";
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				objectivesList.setTitle(resultSet.getString("TITLE"));
				objectivesList.setMetric(resultSet.getString("METRIC"));
				objectivesList.getMetricValueList().add(resultSet.getString("METRIC_VALUE"));
				objectivesList.getYoyList().add(resultSet.getString("Y_O_Y"));
				objectivesList.getPeriodList().add(resultSet.getString("PERIOD"));
				objectivesList.getPeriodValueList().add(resultSet.getString("PERIOD_VALUE"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return objectivesList;
	}
	
	//=================================================
	
	public Set<NameValue> getQmsRewardId(String reward) {
		Set<NameValue> dataSet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();	
			resultSet = statement.executeQuery("select distinct * from QMS_REF_REWARD where REWARD = '"+reward+"'");
			String data = null;
			NameValue nameValue = null;
			while (resultSet.next()) {
				data = resultSet.getString("REWARD");
				if(data != null && !data.trim().isEmpty() && !data.equalsIgnoreCase("n/a") && !data.equalsIgnoreCase("#n/a")) {
					nameValue = new NameValue();
					nameValue.setName(resultSet.getString("REWARD"));
					nameValue.setValue(resultSet.getString("REWARD_ID"));
					dataSet.add(nameValue);			
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return dataSet;		
	}
	

	@Override
	public Set<NameValue> getQmsQualityMeasureId(String memberId) {
		Set<NameValue> dataSet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();	
			resultSet = statement.executeQuery("select distinct q.quality_measure_id, q.measure_name,l.status"
					+ " from QMS_QUALITY_MEASURE q"
					+ " inner join QMS_GIC_LIFECYCLE l on l.QUALITY_MEASURE_ID=q.quality_measure_id"
					+ " where l.member_id = '"+memberId+"'");
			String data = null;
			NameValue nameValue = null;
			while (resultSet.next()) {
				data = resultSet.getString("measure_name");
				if(data != null && !data.trim().isEmpty() && !data.equalsIgnoreCase("n/a") && !data.equalsIgnoreCase("#n/a")) {
					nameValue = new NameValue();
					nameValue.setName(resultSet.getString("measure_name"));
					nameValue.setValue(resultSet.getString("quality_measure_id"));
					dataSet.add(nameValue);			
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return dataSet;		
	}
	
	@Override
	public Set<RefPhysicalActivity> getQmsRefPhysicalActivityFrequency(String goal) {
		Set<RefPhysicalActivity> dataSet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from QMS_REF_PHYSICAL_ACTIVITY where GOAL='"+goal+"'");
			RefPhysicalActivity qmsRefPhysicalActivity = null;
			while (resultSet.next()) {
					qmsRefPhysicalActivity = new RefPhysicalActivity();
					qmsRefPhysicalActivity.setFrequency(resultSet.getString("FREQUENCY"));
					dataSet.add(qmsRefPhysicalActivity);			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return dataSet;		
	}
	
	@Override
	public Set<RefPhysicalActivity> getQmsRefPhysicalActivity(String goal, String frequency) {
		Set<RefPhysicalActivity> dataSet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from QMS_REF_PHYSICAL_ACTIVITY where GOAL='"+goal+"' and FREQUENCY='"+frequency+"'");
			RefPhysicalActivity qmsRefPhysicalActivity = null;
			while (resultSet.next()) {
					qmsRefPhysicalActivity = new RefPhysicalActivity();
					qmsRefPhysicalActivity.setRefId(resultSet.getString("REF_ID"));
					qmsRefPhysicalActivity.setGoal(resultSet.getString("GOAL"));
					qmsRefPhysicalActivity.setFrequency(resultSet.getString("FREQUENCY"));
					dataSet.add(qmsRefPhysicalActivity);			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return dataSet;		
	}
	
	@Override
	public Set<RefCalorieIntake> getQmsRefCalorieIntake(String goal, String frequency) {
		Set<RefCalorieIntake> dataSet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from QMS_REF_CALORIE_INTAKE where GOAL='"+goal+"' and FREQUENCY='"+frequency+"'");
			RefCalorieIntake qmsRefCalorieIntake = null;
			while (resultSet.next()) {
					qmsRefCalorieIntake = new RefCalorieIntake();
					qmsRefCalorieIntake.setRefId(resultSet.getString("REF_ID"));
					qmsRefCalorieIntake.setGoal(resultSet.getString("GOAL"));
					qmsRefCalorieIntake.setFrequency(resultSet.getString("FREQUENCY"));
					dataSet.add(qmsRefCalorieIntake);			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return dataSet;		
	}
	
	public Set<String> getCareGapListByMemberId(String memberId) {
		Set<String> dataSet = new HashSet<>();

		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();

			resultSet = statement.executeQuery("select distinct q.quality_measure_id, q.measure_name,l.status"
											+ " from QMS_QUALITY_MEASURE q"
											+ " inner join QMS_GIC_LIFECYCLE l on l.QUALITY_MEASURE_ID=q.quality_measure_id"
											+ " where l.member_id = '"+memberId+"'");
			String measureName = null;
			String status = null;
			while (resultSet.next()) {
				status = resultSet.getString("status");
				if(status != null && !status.equalsIgnoreCase("Close by Payer")) {
					measureName=resultSet.getString("measure_name");				
					dataSet.add(measureName);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return dataSet;
	}
	
	
	@Override
	public FactGoalInterventions getFactGoalInterventions(String memberId) {
		FactGoalInterventions factGoalInterventions = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select * from QMS_FACT_GOAL_INTERVENTIONS where MEMBER_ID='"+memberId+"' ";
			resultSet = statement.executeQuery(query);		
			while (resultSet.next()) {
				factGoalInterventions = new FactGoalInterventions();	
				factGoalInterventions.setGoalSetId(resultSet.getString("GOAL_SET_ID"));
				factGoalInterventions.setMemberId(resultSet.getString("MEMBER_ID"));
				factGoalInterventions.setName(resultSet.getString("NAME"));
				factGoalInterventions.setAge(resultSet.getString("AGE"));
				factGoalInterventions.setGender(resultSet.getString("GENDER"));
				factGoalInterventions.setPersona(resultSet.getString("PERSONA"));
				factGoalInterventions.setPreferredGoal(resultSet.getString("PREFERRED_GOAL"));
				factGoalInterventions.setWeight(resultSet.getString("WEIGHT"));
				factGoalInterventions.setCurrentCalorieIntake(resultSet.getString("CURRENT_CALORIE_INTAKE"));
				factGoalInterventions.setNumberOfChronicDiseases(resultSet.getString("NUMBER_OF_CHRONIC_DISEASES"));
				factGoalInterventions.setAddictions(resultSet.getString("ADDICTIONS"));
				factGoalInterventions.setPhysicalActivityId(resultSet.getString("PHYSICAL_ACTIVITY_ID"));
				factGoalInterventions.setPhysicalActivityGoal(resultSet.getString("PHYSICAL_ACTIVITY_GOAL"));
				factGoalInterventions.setPhysicalActivityFrequency(resultSet.getString("PHYSICAL_ACTIVITY_FREQUENCY"));
				factGoalInterventions.setPhysicalActivityDate(resultSet.getString("PHYSICAL_ACTIVITY_DATE"));
				factGoalInterventions.setCalorieIntakeId(resultSet.getString("CALORIE_INTAKE_ID"));
				factGoalInterventions.setCalorieIntakeGoal(resultSet.getString("CALORIE_INTAKE_GOAL"));
				factGoalInterventions.setCalorieIntakeFrequency(resultSet.getString("CALORIE_INTAKE_FREQUENCY"));
				factGoalInterventions.setCalorieIntakeDate(resultSet.getString("CALORIE_INTAKE_DATE"));
				factGoalInterventions.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
				factGoalInterventions.setCareGap(resultSet.getString("CARE_GAP"));
				factGoalInterventions.setCareGapDate(resultSet.getString("CARE_GAP_DATE"));
				factGoalInterventions.setInterventions(resultSet.getString("INTERVENTIONS"));
				factGoalInterventions.setCurrFlag(resultSet.getString("CURR_FLAG"));
				factGoalInterventions.setRecCreateDate(resultSet.getString("REC_CREATE_DATE"));
				factGoalInterventions.setRecUpdateDate(resultSet.getString("REC_UPDATE_DATE"));
				factGoalInterventions.setLatestFlag(resultSet.getString("LATEST_FLAG"));
				factGoalInterventions.setActiveFlag(resultSet.getString("ACTIVE_FLAG"));
				factGoalInterventions.setIngestionDate(resultSet.getString("INGESTION_DATE"));
				factGoalInterventions.setSourceName(resultSet.getString("SOURCE_NAME"));
				factGoalInterventions.setUserName(resultSet.getString("USER_NAME"));
				}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return factGoalInterventions;
	}
	

	@Override
	public RestResult updateFactGoalInterventions(FactGoalInterventions factGoalInterventions) {
		String sqlStatement = 			
				"update QMS_FACT_GOAL_INTERVENTIONS set"
				+"GOAL_SET_ID=?,MEMBER_ID=?,NAME=?,AGE=?,GENDER=?,PERSONA=?,"
				+"PREFERRED_GOAL=?,WEIGHT=?,CURRENT_CALORIE_INTAKE=?,"
				+"NUMBER_OF_CHRONIC_DISEASES=?,ADDICTIONS=?,PHYSICAL_ACTIVITY_ID=?,"
				+"PHYSICAL_ACTIVITY_GOAL=?,PHYSICAL_ACTIVITY_FREQUENCY=?,"
				+"PHYSICAL_ACTIVITY_DATE=?,CALORIE_INTAKE_ID=?,CALORIE_INTAKE_GOAL=?,"
				+"CALORIE_INTAKE_FREQUENCY=?,CALORIE_INTAKE_DATE=?,QUALITY_MEASURE_ID=?,"
				+"CARE_GAP=?,CARE_GAP_DATE=?,INTERVENTIONS=?";
					
		PreparedStatement statement = null;
		Connection connection = null;
		User userData1 = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);

		try {
			connection = qmsConnection.getOracleConnection();
			int i = 0;
			Date date = new Date();
			Timestamp timestamp = new Timestamp(date.getTime());
			statement = connection.prepareStatement(sqlStatement);
			statement.setString(++i, factGoalInterventions.getGoalSetId());
			statement.setString(++i, factGoalInterventions.getMemberId());
			statement.setString(++i, factGoalInterventions.getName());
			statement.setString(++i, factGoalInterventions.getAge());
			statement.setString(++i, factGoalInterventions.getGender());
			statement.setString(++i, factGoalInterventions.getPersona());
			statement.setString(++i, factGoalInterventions.getPreferredGoal());
			statement.setString(++i, factGoalInterventions.getWeight());
			statement.setString(++i, factGoalInterventions.getCurrentCalorieIntake());
			statement.setString(++i, factGoalInterventions.getNumberOfChronicDiseases());
			statement.setString(++i, factGoalInterventions.getAddictions());
			statement.setString(++i, factGoalInterventions.getPhysicalActivityId());
			statement.setString(++i, factGoalInterventions.getPhysicalActivityGoal());
			statement.setString(++i, factGoalInterventions.getPhysicalActivityFrequency());
			statement.setString(++i, factGoalInterventions.getPhysicalActivityDate());
			statement.setString(++i, factGoalInterventions.getCalorieIntakeId());
			statement.setString(++i, factGoalInterventions.getCalorieIntakeGoal());
			statement.setString(++i, factGoalInterventions.getCalorieIntakeFrequency());
			statement.setString(++i, factGoalInterventions.getCalorieIntakeDate());
			statement.setString(++i, factGoalInterventions.getQualityMeasureId());
			statement.setString(++i, factGoalInterventions.getCareGapDate());
			statement.setString(++i, factGoalInterventions.getInterventions());
			statement.setString(++i, "Y");
			statement.setTimestamp(++i, timestamp);
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i,"Y");
			statement.setString(++i,"A");
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i, "UI");
			if (userData1 != null && userData1.getName() != null)
				statement.setString(++i, userData1.getName());
			else
				statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
			statement.executeUpdate();
			return RestResult.getSucessRestResult("Fact Goal Interventions update sucess. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		} finally {
			qmsConnection.closeJDBCResources(null, statement, connection);
		}
	}	

	@Override
	public FactGoalRecommendations getFactGoalRecommendations(String memberId) {
		FactGoalRecommendations factGoalRecommendations = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select * from QMS_FACT_GOAL_RECOMMENDATIONS where MEMBER_ID='"+memberId+"'";
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				factGoalRecommendations = new FactGoalRecommendations();
				factGoalRecommendations.setRecommendationId(resultSet.getString("RECOMMENDATION_ID"));
				factGoalRecommendations.setMemberId(resultSet.getString("MEMBER_ID"));
				factGoalRecommendations.setName(resultSet.getString("NAME"));
				factGoalRecommendations.setAge(resultSet.getString("AGE"));
				factGoalRecommendations.setGender(resultSet.getString("GENDER"));
				factGoalRecommendations.setPhysicalActivityGoal(resultSet.getString("PHYSICAL_ACTIVITY_GOAL"));
				factGoalRecommendations.setPhysicalActivityFrequency(resultSet.getString("PHYSICAL_ACTIVITY_FREQUENCY"));
				factGoalRecommendations.setPhysicalActivityDate(resultSet.getString("PHYSICAL_ACTIVITY_DATE"));
				factGoalRecommendations.setCalorieIntakeGoal(resultSet.getString("CALORIE_INTAKE_GOAL"));
				factGoalRecommendations.setCalorieIntakeFrequency(resultSet.getString("CALORIE_INTAKE_FREQUENCY"));
				factGoalRecommendations.setCalorieIntakeDate(resultSet.getString("CALORIE_INTAKE_DATE"));
				factGoalRecommendations.setCareGap(resultSet.getString("CARE_GAP"));
				factGoalRecommendations.setCareGapDate(resultSet.getString("CARE_GAP_DATE"));
				factGoalRecommendations.setPersona(resultSet.getString("PERSONA"));
				factGoalRecommendations.setPreferredGoal(resultSet.getString("PREFERRED_GOAL"));
				factGoalRecommendations.setCurrentCalorieIntake(resultSet.getString("CURRENT_CALORIE_INTAKE"));
				factGoalRecommendations.setNumberOfChronicDiseases(resultSet.getString("NUMBER_OF_CHRONIC_DISEASES"));
				factGoalRecommendations.setAddictions(resultSet.getString("ADDICTIONS"));
				factGoalRecommendations.setPhysicalActivityId(resultSet.getString("PHYSICAL_ACTIVITY_ID"));
				factGoalRecommendations.setCalorieIntakeId(resultSet.getString("CALORIE_INTAKE_ID"));
				factGoalRecommendations.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return factGoalRecommendations;
	}

	@Override
	public RestResult updateFactGoalRecommendations(FactGoalRecommendations factGoalRecommendations) {

		String sqlStatement = 
				"update QMS_FACT_GOAL_RECOMMENDATIONS set "
				+ "PHYSICAL_ACTIVITY_GOAL=?,PHYSICAL_ACTIVITY_FREQUENCY=?,PHYSICAL_ACTIVITY_DATE=?,"
				+ "CALORIE_INTAKE_GOAL=?,CALORIE_INTAKE_FREQUENCY=?,CALORIE_INTAKE_DATE=?,"
				+ "CARE_GAP=?,CARE_GAP_DATE=?,,PHYSICAL_ACTIVITY_ID=?,CALORIE_INTAKE_ID=?,"
				+ "QUALITY_MEASURE_ID=? where MEMBER_ID=?";				
		PreparedStatement statement = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			int i = 0;
			statement = connection.prepareStatement(sqlStatement);
			statement.setString(++i, factGoalRecommendations.getPhysicalActivityGoal());
			statement.setString(++i, factGoalRecommendations.getPhysicalActivityFrequency());
			statement.setString(++i, factGoalRecommendations.getPhysicalActivityDate());
			statement.setString(++i, factGoalRecommendations.getCalorieIntakeGoal());
			statement.setString(++i, factGoalRecommendations.getCalorieIntakeFrequency());
			statement.setString(++i, factGoalRecommendations.getCalorieIntakeDate());
			statement.setString(++i, factGoalRecommendations.getCareGap());
			statement.setString(++i, factGoalRecommendations.getCareGapDate());
			statement.setString(++i, factGoalRecommendations.getMemberId());
			statement.setString(++i, factGoalRecommendations.getPhysicalActivityId());
			statement.setString(++i, factGoalRecommendations.getCalorieIntakeId());
			statement.setString(++i, factGoalRecommendations.getQualityMeasureId());
			statement.executeUpdate();
			
			return RestResult.getSucessRestResult("Fact Goal Recommendations update sucess. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		} finally {
			qmsConnection.closeJDBCResources(null, statement, connection);
		}
	}	
	
	@Override
	public RestResult insertFactGoalRecommendationsCreator(FactGoalRecommendations factGoalRecommendations) {

		String sqlStatementInsert = 
				"insert into QMS_FACT_GOAL_RECOMMENDATIONS(RECOMMENDATION_ID,MEMBER_ID,NAME,AGE,GENDER"
				+ ",PERSONA,PREFERRED_GOAL,CURRENT_CALORIE_INTAKE,NUMBER_OF_CHRONIC_DISEASES"
				+ ",ADDICTIONS,PHYSICAL_ACTIVITY_GOAL,PHYSICAL_ACTIVITY_FREQUENCY,PHYSICAL_ACTIVITY_DATE"
				+ ",CALORIE_INTAKE_GOAL,CALORIE_INTAKE_FREQUENCY,CALORIE_INTAKE_DATE,CARE_GAP,CARE_GAP_DATE"
				+ ",CURR_FLAG,REC_CREATE_DATE,REC_UPDATE_DATE,LATEST_FLAG,ACTIVE_FLAG,INGESTION_DATE,"
				+ "SOURCE_NAME,USER_NAME,PHYSICAL_ACTIVITY_ID,CALORIE_INTAKE_ID,QUALITY_MEASURE_ID)"
				+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				
		PreparedStatement statement = null;
		Connection connection = null;
		ResultSet resultSet = null;
		Statement getStatement = null;
		User userData1 = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);

		try {
			connection = qmsConnection.getOracleConnection();
			int factGoalRecId = 0;
			getStatement = connection.createStatement();
			
			resultSet = getStatement.executeQuery("select max(RECOMMENDATION_ID) from QMS_FACT_GOAL_RECOMMENDATIONS");
			while (resultSet.next()) {
				factGoalRecId = resultSet.getInt(1);
			}
			factGoalRecId = factGoalRecId + 1;
			
			int i = 0;
			Date date = new Date();
			Timestamp timestamp = new Timestamp(date.getTime());
			statement = connection.prepareStatement(sqlStatementInsert);
			
			statement.setInt(++i, factGoalRecId);
			statement.setString(++i, factGoalRecommendations.getMemberId());
			statement.setString(++i, factGoalRecommendations.getName());
			statement.setString(++i, factGoalRecommendations.getAge());
			statement.setString(++i, factGoalRecommendations.getGender());
			statement.setString(++i, factGoalRecommendations.getPhysicalActivityGoal());
			statement.setString(++i, factGoalRecommendations.getPhysicalActivityFrequency());
			statement.setString(++i, factGoalRecommendations.getPhysicalActivityDate());
			statement.setString(++i, factGoalRecommendations.getCalorieIntakeGoal());
			statement.setString(++i, factGoalRecommendations.getCalorieIntakeFrequency());
			statement.setString(++i, factGoalRecommendations.getCalorieIntakeDate());
			statement.setString(++i, factGoalRecommendations.getCareGap());
			statement.setString(++i, factGoalRecommendations.getCareGapDate());
			statement.setString(++i, factGoalRecommendations.getPersona());
			statement.setString(++i, factGoalRecommendations.getPreferredGoal());
			statement.setString(++i, factGoalRecommendations.getCurrentCalorieIntake());
			statement.setString(++i, factGoalRecommendations.getNumberOfChronicDiseases());
			statement.setString(++i, factGoalRecommendations.getAddictions());
			statement.setString(++i, factGoalRecommendations.getPhysicalActivityId());
			statement.setString(++i, factGoalRecommendations.getCalorieIntakeId());
			statement.setString(++i, factGoalRecommendations.getQualityMeasureId());
			statement.setString(++i, "Y");
			statement.setTimestamp(++i, timestamp);
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i,"Y");
			statement.setString(++i,"A");
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i, "UI");
			if (userData1 != null && userData1.getName() != null)
				statement.setString(++i, userData1.getName());
			else
				statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
			statement.executeUpdate();

			return RestResult.getSucessRestResult("Fact Goal Recommendations creation sucess. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		} finally {
			qmsConnection.closeJDBCResources(resultSet, getStatement, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}
	}
	
	@Override
	public PersonaMemberListView getPersonaMemberList(String mid) {
		PersonaMemberListView personaMemberList = new PersonaMemberListView();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select MEMBER_ID,PERSONA_NAME,GOALS,MEASURE_CALORIE_INTAKE"
					+ ",COMORBIDITY_COUNT,ADDICTIONS,REWARDS,MOTIVATIONS,age,weight "
					+ "from PERSONA_MEMBERLIST_VIEW where member_id='"+mid+"'";

			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				personaMemberList.setMemberId(resultSet.getString("MEMBER_ID"));
				personaMemberList.setPersonaName(resultSet.getString("PERSONA_NAME"));
				personaMemberList.setGoals(resultSet.getString("GOALS"));
				personaMemberList.setAge(resultSet.getString("age"));
				personaMemberList.setWeight(resultSet.getString("weight"));
				personaMemberList.setMeasureCalorieIntake(resultSet.getString("MEASURE_CALORIE_INTAKE"));
				personaMemberList.setComorbidityCount(resultSet.getString("COMORBIDITY_COUNT"));
				personaMemberList.setAddictions(resultSet.getString("ADDICTIONS"));
				personaMemberList.setRewards(resultSet.getString("REWARDS"));
				personaMemberList.setMotivations(resultSet.getString("MOTIVATIONS"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return personaMemberList;
	}

	@Override
	public Set<String> getDataByTableAndColumn(String tableName, String columnName) {
		Set<String> objectives = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select DISTINCT "+columnName+" from "+tableName;
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				objectives.add(resultSet.getString(columnName));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return objectives;
	}

	@Override
	public RestResult updateRewards(String memberId, List<String> rewards) {

//		String sqlStatementInsert = "UPSERT into QMS.ENROLLMENT_FILE_OUTPUT (REMARKS,VERIFYFLAG,CRMFLAG) "
//				+ "values (?,?,?) where MEMBER_ID=?";		
		String sqlStatementInsert = "UPDATE QMS_ENROLLMENT_FILE_OUTPUT SET REWARD1=?,REWARD2=?,REWARD3=? "
				+ "where MEMBER_ID=?";		
		
		PreparedStatement statement = null;
		Connection connection = null;
		try {	
			connection = qmsConnection.getOracleConnection();
			statement = connection.prepareStatement(sqlStatementInsert);			
			int i=0;					
			String[] rewardsAry = (String[]) rewards.toArray();
			statement.setString(++i, rewardsAry.length>0?rewardsAry[0]:null);
			statement.setString(++i, rewardsAry.length>1?rewardsAry[1]:null);
			statement.setString(++i, rewardsAry.length>2?rewardsAry[2]:null);
			statement.setString(++i, memberId);
			statement.executeUpdate();
			//connection.commit();
			return RestResult.getSucessRestResult(" Rewards update Success. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(null, statement, connection);
		}		
	}

	@Override
	public Set<String> getAllRewards(String questionId) {
		Set<String> dataSet = new HashSet<>();

		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery("select answer from ref_survey_answers where question_id="+questionId);
			
			while (resultSet.next()) {
				dataSet.add(resultSet.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return dataSet;
	}	
	
	@Override
	public List<RewardsFileOutput> getRewardsFileOutputList(String memberId) {
		List<RewardsFileOutput> rewardsFileOutputList = new ArrayList<>();
		RewardsFileOutput rewardsFileOutput = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select REWARD_ID,MEMBER_ID,NAME,AGE,GENDER,WEIGHT,PERSONA,PREFERRED_REWARD,"
					+ "MOTIVATIONS,CATEGORY,GOAL,FREQUENCY,GOAL_DATE,REWARD1,REWARD2,REWARD3,Recommended_Reward "
					+ "from QMS_REWARDS_FILE_OUTPUT where MEMBER_ID='"+memberId+"'";
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				rewardsFileOutput = new RewardsFileOutput();
				rewardsFileOutput.setRewardId(resultSet.getString("REWARD_ID"));
				rewardsFileOutput.setMemberId(resultSet.getString("MEMBER_ID"));
				rewardsFileOutput.setName(resultSet.getString("NAME"));
				rewardsFileOutput.setAge(resultSet.getString("AGE"));
				rewardsFileOutput.setGender(resultSet.getString("GENDER"));
				rewardsFileOutput.setWeight(resultSet.getString("WEIGHT"));
				rewardsFileOutput.setPersona(resultSet.getString("PERSONA"));
				rewardsFileOutput.setPreferredReward(resultSet.getString("PREFERRED_REWARD"));
				rewardsFileOutput.setMotivations(resultSet.getString("MOTIVATIONS"));
				rewardsFileOutput.setCategory(resultSet.getString("CATEGORY"));
				rewardsFileOutput.setGoal(resultSet.getString("GOAL"));
				rewardsFileOutput.setFrequency(resultSet.getString("FREQUENCY"));
				rewardsFileOutput.setGoalDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("GOAL_DATE")));
				rewardsFileOutput.setReward1(resultSet.getString("REWARD1"));
				rewardsFileOutput.setReward2(resultSet.getString("REWARD2"));
				rewardsFileOutput.setReward3(resultSet.getString("REWARD3"));
				rewardsFileOutput.setRecommendedReward(resultSet.getString("Recommended_Reward"));
				//rewardsFileOutputList.setOthers(resultSet.getString("OTHERS"));
				rewardsFileOutputList.add(rewardsFileOutput);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return rewardsFileOutputList;
	}

	@Override
	public RestResult insertRewardsFileOutput(List<RewardsFileOutput> rewardsFileOutputList) {
		String sqlStatementInsert = 
				"insert into QMS_REWARDS_FILE_OUTPUT (REWARD_ID,MEMBER_ID,NAME,AGE,GENDER,WEIGHT,"+ 
				"PERSONA,PREFERRED_REWARD,MOTIVATIONS,CATEGORY,GOAL,FREQUENCY,GOAL_DATE,Recommended_Reward,"+
				"REWARD1,REWARD2,REWARD3,"+
				"curr_flag,rec_create_date,rec_update_date,latest_flag,active_flag,ingestion_date,source_name,user_name) "+
				"values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement statement = null;
		Connection connection = null;
		ResultSet resultSet = null;
		Statement getStatement = null;
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		if(rewardsFileOutputList == null || rewardsFileOutputList.isEmpty()) 
			return RestResult.getFailRestResult(" RewardsFileOutputList is null or empty ");
		
		String memberId = rewardsFileOutputList.get(0).getMemberId();
		try {
			connection = qmsConnection.getOracleConnection();
			connection.setAutoCommit(false);
			getStatement = connection.createStatement();
			getStatement.executeUpdate("delete from QMS_REWARDS_FILE_OUTPUT where MEMBER_ID="+memberId);			
			
			int rewardId = 0;
			resultSet = getStatement.executeQuery("select max(REWARD_ID) from QMS_REWARDS_FILE_OUTPUT");
			while (resultSet.next()) {
				rewardId = resultSet.getInt(1);
			}
			
			int i = 0;
			Date date = new Date();
			Timestamp timestamp = new Timestamp(date.getTime());
			statement = connection.prepareStatement(sqlStatementInsert);
			for (RewardsFileOutput rewardsFileOutput : rewardsFileOutputList) {
				i=0;
				rewardId=rewardId+1;
				statement.setInt(++i, rewardId);
				statement.setInt(++i, Integer.parseInt(rewardsFileOutput.getMemberId()));
				statement.setString(++i, rewardsFileOutput.getName());
				statement.setString(++i, rewardsFileOutput.getAge());
				statement.setString(++i, rewardsFileOutput.getGender());
				statement.setString(++i, rewardsFileOutput.getWeight());
				statement.setString(++i, rewardsFileOutput.getPersona());
				statement.setString(++i, rewardsFileOutput.getPreferredReward());
				statement.setString(++i, rewardsFileOutput.getMotivations());
				statement.setString(++i, rewardsFileOutput.getCategory());
				statement.setString(++i, rewardsFileOutput.getGoal());
				statement.setString(++i, rewardsFileOutput.getFrequency());
				statement.setString(++i, rewardsFileOutput.getGoalDate());
				statement.setString(++i, rewardsFileOutput.getRecommendedReward());
				statement.setString(++i, rewardsFileOutput.getReward1());
				statement.setString(++i, rewardsFileOutput.getReward2());
				statement.setString(++i, rewardsFileOutput.getReward3());
				
				statement.setString(++i, "Y");
				statement.setTimestamp(++i, timestamp);
				statement.setTimestamp(++i, timestamp);
				statement.setString(++i, "Y");
				statement.setString(++i, "A");
				statement.setTimestamp(++i, timestamp);
				statement.setString(++i, "UI");
				if (userData != null && userData.getName() != null)
					statement.setString(++i, userData.getName());
				else
					statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
				statement.addBatch();
			}
			statement.executeBatch();
			return RestResult.getSucessRestResult("Fact Goal Recommendations creation sucess. ");
		} catch (Exception e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
				return RestResult.getFailRestResult(e1.getMessage());
			}
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		} finally {
			qmsConnection.closeJDBCResources(resultSet, getStatement, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}
	}	
	
	@Override
	public RestResult insertRewardsRecommendations(RewardsRecommendations rewardsRecommendations) {
		String sqlStatementInsert = "insert into QMS_REWARDS_RECOMMENDATIONS"
									+ "(REWARD_REC_ID,MEMBER_ID,NAME,AGE,GENDER,WEIGHT," 
									+ "PERSONA,PREFERRED_REWARD,MOTIVATIONS,CATEGORY,"
									+ "GOAL,FREQUENC,GOAL_DATE,REWARD,REWARD_ID) "
									+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		PreparedStatement statement = null;
		Connection connection = null;
		ResultSet resultSet = null;
		Statement getStatement = null;
		User userData1 = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		try {
			connection = qmsConnection.getOracleConnection();
			/*
			int factGoalRecId = 0;
			getStatement = connection.createStatement();
			resultSet = getStatement.executeQuery("select max(RECOMMENDATION_ID)from QMS_REWARDS_RECOMMENDATIONS");
			while (resultSet.next()) {
				factGoalRecId = resultSet.getInt(1);
			}
			factGoalRecId = factGoalRecId + 1;
			*/
			int i = 0;
			Date date = new Date();
			Timestamp timestamp = new Timestamp(date.getTime());
			statement = connection.prepareStatement(sqlStatementInsert);
			//statement.setInt(++i, factGoalRecId);
			statement.setString(++i, rewardsRecommendations.getRewardRecId());
			statement.setString(++i, rewardsRecommendations.getMemberId());
			statement.setString(++i, rewardsRecommendations.getName());
			statement.setString(++i, rewardsRecommendations.getAge());
			statement.setString(++i, rewardsRecommendations.getGender());
			statement.setString(++i, rewardsRecommendations.getWeight());
			statement.setString(++i, rewardsRecommendations.getPersona());
			statement.setString(++i, rewardsRecommendations.getPreferredReward());
			statement.setString(++i, rewardsRecommendations.getMotivations());
			statement.setString(++i, rewardsRecommendations.getCategory());
			statement.setString(++i, rewardsRecommendations.getGoal());
			statement.setString(++i, rewardsRecommendations.getFrequency());
			statement.setString(++i, rewardsRecommendations.getGoalDate());
			statement.setString(++i, rewardsRecommendations.getReward());

			statement.setString(++i, "Y");
			statement.setTimestamp(++i, timestamp);
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i, "Y");
			statement.setString(++i, "A");
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i, "UI");
			if (userData1 != null && userData1.getName() != null)
				statement.setString(++i, userData1.getName());
			else
				statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
			statement.setString(++i, rewardsRecommendations.getRewardId());			
			statement.executeUpdate();
			// httpSession.setAttribute(QMSConstants.SESSION_PAT_ID,
			// factGoalRecId + "");
			return RestResult.getSucessRestResult("Fact Goal Recommendations creation sucess. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		} finally {
			qmsConnection.closeJDBCResources(resultSet, getStatement, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}
	}

	@Override
	public Set<PersonaMemberListView> filterPersonaMemberList(String filterType, String filterValue) {
		Set<PersonaMemberListView> listPersonaMemberList = new HashSet<>();
		PersonaMemberListView personaMemberList = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		if(!filterType.equalsIgnoreCase("cluster") && !filterType.equalsIgnoreCase("persona"))
			return listPersonaMemberList;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select * from PERSONA_MEMBERLIST_VIEW where PERSONA_NAME='"+filterValue+"'";
			if(filterType.equalsIgnoreCase("cluster"))
			query = "select * from PERSONA_MEMBERLIST_VIEW where cluster_id='"+filterValue+"'";

			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				personaMemberList = new PersonaMemberListView();
				personaMemberList.setMemberId(resultSet.getString("MEMBER_ID"));
				personaMemberList.setPersonaName(resultSet.getString("PERSONA_NAME"));
				personaMemberList.setGoals(resultSet.getString("GOALS"));
				personaMemberList.setMeasureCalorieIntake(resultSet.getString("MEASURE_CALORIE_INTAKE"));
				personaMemberList.setComorbidityCount(resultSet.getString("COMORBIDITY_COUNT"));
				personaMemberList.setAddictions(resultSet.getString("ADDICTIONS"));
				personaMemberList.setRewards(resultSet.getString("REWARDS"));
				personaMemberList.setMotivations(resultSet.getString("MOTIVATIONS"));
				
				personaMemberList.setModeOfContact(resultSet.getString("MODE_OF_CONTACT"));    
				personaMemberList.setFormOfExercise(resultSet.getString("FORM_OF_EXERCISE"));      
				personaMemberList.setFrequencyOfExcercise(resultSet.getString("FREQUENCY_OF_EXCERCISE"));       
				personaMemberList.setIdealHealthGoal(resultSet.getString("IDEAL_HEALTH_GOAL"));
				personaMemberList.setSocialMediaUsage(resultSet.getString("SOCIAL_MEDIA_USAGE"));                
				personaMemberList.setAge(resultSet.getString("AGE"));      
				personaMemberList.setGender(resultSet.getString("GENDER"));
				personaMemberList.setEthnicity(resultSet.getString("ETHNICITY"));
				personaMemberList.setClusterId(resultSet.getString("CLUSTER_ID"));				
				
				listPersonaMemberList.add(personaMemberList);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return listPersonaMemberList;
	}

	@Override
	public Set<GoalRecommendationSet> getGoalRecommendationsSetMemberList() {
		Map<String, GoalRecommendationSet> goalRecommendationSetMap = 
				new HashMap<String, GoalRecommendationSet>(); 
		Set<GoalRecommendationSet> goalRecommendationListSet = new HashSet<>();
		GoalRecommendationSet goalRecommendationSet = null;
		GoalRecommendationSetData goalRecommendationSetData = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select * from Qms_Fact_Goal_Recommendations";
			resultSet = statement.executeQuery(query);
			String memberId = null;
			while (resultSet.next()) {
				memberId = resultSet.getString("MEMBER_ID");
				goalRecommendationSet = new GoalRecommendationSet();
				goalRecommendationSetData = new GoalRecommendationSetData();
				goalRecommendationSet.setGoalRecommendation(goalRecommendationSetData);
				goalRecommendationSetMap.put(memberId, goalRecommendationSet);

				goalRecommendationSet.setMemberId(resultSet.getString("MEMBER_ID"));
				goalRecommendationSet.setName(resultSet.getString("NAME"));
				goalRecommendationSet.setAge(resultSet.getString("AGE"));
				goalRecommendationSet.setGender(resultSet.getString("GENDER"));
				goalRecommendationSet.setPersona(resultSet.getString("PERSONA"));
				goalRecommendationSetData.setPhysicalActivityGoal(resultSet.getString("PHYSICAL_ACTIVITY_GOAL"));
				goalRecommendationSetData.setPhysicalActivityFrequency(resultSet.getString("PHYSICAL_ACTIVITY_FREQUENCY"));
				goalRecommendationSetData.setPhysicalActivityDate(resultSet.getString("PHYSICAL_ACTIVITY_DATE"));
				goalRecommendationSetData.setCalorieIntakeGoal(resultSet.getString("CALORIE_INTAKE_GOAL"));
				goalRecommendationSetData.setCalorieIntakeFrequency(resultSet.getString("CALORIE_INTAKE_FREQUENCY"));
				goalRecommendationSetData.setCalorieIntakeDate(resultSet.getString("CALORIE_INTAKE_DATE"));
				goalRecommendationSetData.setCareGap(resultSet.getString("CARE_GAP"));
				goalRecommendationSetData.setCareGapDate(resultSet.getString("CARE_GAP_DATE"));				
			}
			resultSet.close();
			
			query = "select * from QMS_FACT_GOAL_INTERVENTIONS";
			resultSet = statement.executeQuery(query);			
			while (resultSet.next()) {
				memberId = resultSet.getString("MEMBER_ID");
				goalRecommendationSet = goalRecommendationSetMap.get(memberId);
				if(goalRecommendationSet == null) {
					goalRecommendationSet = new GoalRecommendationSet();
					goalRecommendationSetMap.put(memberId, goalRecommendationSet);
					goalRecommendationSet.setMemberId(resultSet.getString("MEMBER_ID"));
					goalRecommendationSet.setName(resultSet.getString("NAME"));
					goalRecommendationSet.setAge(resultSet.getString("AGE"));
					goalRecommendationSet.setGender(resultSet.getString("GENDER"));
					goalRecommendationSet.setPersona(resultSet.getString("PERSONA"));					
				} 
				goalRecommendationSetData = new GoalRecommendationSetData();
				goalRecommendationSet.setGoalSet(goalRecommendationSetData);
				goalRecommendationSetData.setPhysicalActivityGoal(resultSet.getString("PHYSICAL_ACTIVITY_GOAL"));
				goalRecommendationSetData.setPhysicalActivityFrequency(resultSet.getString("PHYSICAL_ACTIVITY_FREQUENCY"));
				goalRecommendationSetData.setPhysicalActivityDate(resultSet.getString("PHYSICAL_ACTIVITY_DATE"));
				goalRecommendationSetData.setCalorieIntakeGoal(resultSet.getString("CALORIE_INTAKE_GOAL"));
				goalRecommendationSetData.setCalorieIntakeFrequency(resultSet.getString("CALORIE_INTAKE_FREQUENCY"));
				goalRecommendationSetData.setCalorieIntakeDate(resultSet.getString("CALORIE_INTAKE_DATE"));
				goalRecommendationSetData.setCareGap(resultSet.getString("CARE_GAP"));
				goalRecommendationSetData.setCareGapDate(resultSet.getString("CARE_GAP_DATE"));
			}					
			goalRecommendationListSet.addAll(goalRecommendationSetMap.values());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return goalRecommendationListSet;
	}

	@Override
	public Set<RewardRecommendationSet> getRewardRecommendationsSetMemberList() {
		Map<String, RewardRecommendationSet> rewardRecommendationSetMap = 
				new HashMap<String, RewardRecommendationSet>(); 
		Set<RewardRecommendationSet> rewardRecommendationListSet = new HashSet<>();
		RewardRecommendationSet rewardRecommendationSet = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select * from Qms_Rewards_Recommendations";
			resultSet = statement.executeQuery(query);
			String key = null;
			while (resultSet.next()) {
				key = resultSet.getString("MEMBER_ID")+"#"+
						resultSet.getString("CATEGORY")+"#"+
						resultSet.getString("GOAL")+"#"+
						resultSet.getString("FREQUENCY");
				rewardRecommendationSet = new RewardRecommendationSet();
				rewardRecommendationSetMap.put(key, rewardRecommendationSet);
				
				rewardRecommendationSet.setMemberId(resultSet.getString("MEMBER_ID"));
				rewardRecommendationSet.setName(resultSet.getString("NAME"));
				rewardRecommendationSet.setAge(resultSet.getString("AGE"));
				rewardRecommendationSet.setGender(resultSet.getString("GENDER"));
				rewardRecommendationSet.setPersona(resultSet.getString("PERSONA"));
				rewardRecommendationSet.setMotivations(resultSet.getString("MOTIVATIONS"));
				rewardRecommendationSet.setCategory(resultSet.getString("CATEGORY"));
				rewardRecommendationSet.setGoal(resultSet.getString("GOAL"));
				rewardRecommendationSet.setFrequency(resultSet.getString("FREQUENCY"));
				rewardRecommendationSet.setRewardRecommendation(resultSet.getString("REWARD"));
				rewardRecommendationSet.setRewardId(resultSet.getString("REWARD_ID"));
				
			}
			
			resultSet.close();
			query = "select * from Qms_Rewards_set";
			resultSet = statement.executeQuery(query);			
			while (resultSet.next()) {
				key = resultSet.getString("MEMBER_ID")+"#"+
						resultSet.getString("CATEGORY")+"#"+
						resultSet.getString("GOAL")+"#"+
						resultSet.getString("FREQUENCY");
				rewardRecommendationSet = rewardRecommendationSetMap.get(key);
				if(rewardRecommendationSet == null) {
					rewardRecommendationSet = new RewardRecommendationSet();
					rewardRecommendationSetMap.put(key, rewardRecommendationSet);
					
					rewardRecommendationSet.setMemberId(resultSet.getString("MEMBER_ID"));
					rewardRecommendationSet.setName(resultSet.getString("NAME"));
					rewardRecommendationSet.setAge(resultSet.getString("AGE"));
					rewardRecommendationSet.setGender(resultSet.getString("GENDER"));
					rewardRecommendationSet.setPersona(resultSet.getString("PERSONA"));		
					rewardRecommendationSet.setMotivations(resultSet.getString("MOTIVATIONS"));
					rewardRecommendationSet.setCategory(resultSet.getString("CATEGORY"));
					rewardRecommendationSet.setGoal(resultSet.getString("GOAL"));
					rewardRecommendationSet.setFrequency(resultSet.getString("FREQUENCY"));
					rewardRecommendationSet.setRewardId(resultSet.getString("REWARD_ID"));
				} 
				rewardRecommendationSet.setRewardSet(resultSet.getString("REWARD"));
				rewardRecommendationSet.setRewardId(resultSet.getString("REWARD_ID"));
			}						
			rewardRecommendationListSet.addAll(rewardRecommendationSetMap.values());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return rewardRecommendationListSet;
	}

	@Override
	public Set<NameValue> getCareGapMemberId1(String memberId) {
		// TODO Auto-generated method stub
		return null;
	}
}
