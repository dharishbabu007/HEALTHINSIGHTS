package com.qms.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.EnrollmentFileOutput;
import com.qms.rest.model.FactGoalRecommendations;
import com.qms.rest.model.Objectives;
import com.qms.rest.model.PersonaMemberListView;
import com.qms.rest.model.RefModel;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RewardsFileOutput;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;

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


	public Set<String> getCearGapList(String mid) {
		Set<String> dataSet = new HashSet<>();

		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();

			resultSet = statement.executeQuery("select distinct q.measure_name from QMS_QUALITY_MEASURE q"
					+ " inner join QMS_GIC_LIFECYCLE l on l.QUALITY_MEASURE_ID=q.quality_measure_id"
					+ " where l.member_id = '" + mid + "'");

			String data = null;
			while (resultSet.next()) {
				data = resultSet.getString(1);
				if (data != null && !data.trim().isEmpty() && !data.equalsIgnoreCase("n/a")
						&& !data.equalsIgnoreCase("#n/a"))
					dataSet.add(data);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return dataSet;
	}

	@Override
	public RestResult insertFactGoalRecommendationsCreator(FactGoalRecommendations factGoalRecommendations) {

		String sqlStatementInsert = "insert into QMS_FACT_GOAL_RECOMMENDATIONS(RECOMMENDATION_ID,MEMBER_ID,NAME,AGE,GENDER"
									+ "	,PERSONA,PREFERRED_GOAL,CURRENT_CALORIE_INTAKE"
									+ ",NUMBER_OF_CHRONIC_DISEASES"
									+ ",ADDICTIONS,PHYSICAL_ACTIVITY_GOAL"
									+ ",PHYSICAL_ACTIVITY_FREQUENCY,PHYSICAL_ACTIVITY_DATE"
									+ ",CALORIE_INTAKE_GOAL,CALORIE_INTAKE_FREQUENCY"
									+ ",CALORIE_INTAKE_DATE,CARE_GAP,CARE_GAP_DATE"
									+ ",CURR_FLAG,REC_CREATE_DATE,REC_UPDATE_DATE"
									+ ",LATEST_FLAG,ACTIVE_FLAG,INGESTION_DATE"
									+ ",SOURCE_NAME,USER_NAME)"
									+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				
		PreparedStatement statement = null;
		Connection connection = null;
		ResultSet resultSet = null;
		Statement getStatement = null;
		User userData1 = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);

		try {
			connection = qmsConnection.getOracleConnection();
			int factGoalRecId = 0;
			getStatement = connection.createStatement();
			resultSet = getStatement.executeQuery("select max(RECOMMENDATION_ID)from QMS_FACT_GOAL_RECOMMENDATIONS");
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
	public Set<String> getcareGapMeasurelist(String mid) {
		Set<String> cearGapMeasureList = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery("select distinct q.measure_name from QMS_QUALITY_MEASURE q"
					+ " inner join QMS_GIC_LIFECYCLE l on l.QUALITY_MEASURE_ID=q.quality_measure_id"
					+ " where l.member_id = '" + mid + "'");

			while (resultSet.next()) {
				cearGapMeasureList.add(resultSet.getString("measure_name"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return cearGapMeasureList;
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
			String query = ("select MEMBER_ID,PERSONA_NAME,GOALS,MEASURE_CALORIE_INTAKE"
					+ ",COMORBIDITY_COUNT,ADDICTIONS,REWARDS,MOTIVATIONS "
					+ "from PERSONA_MEMBERLIST_VIEW where member_id = '" + mid + "'");

			resultSet = statement.executeQuery(query);

			while (resultSet.next()) {
				personaMemberList.setMemberId(resultSet.getString("MEMBER_ID"));
				personaMemberList.setPersonaName(resultSet.getString("PERSONA_NAME"));
				personaMemberList.setGoals(resultSet.getString("GOALS"));
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
	public RewardsFileOutput getRewardsFileOutputList(String mid) {
		RewardsFileOutput rewardsFileOutputList = new RewardsFileOutput();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = ("select REWARD_ID,"
					+ "MEMBER_ID,NAME,AGE,GENDER,WEIGHT,PERSONA,PREFERRED_REWARD,MOTIVATIONS,"
					+ "CATEGORY,GOAL,FREQUENCY,GOAL_DATE,REWARD1,REWARD2,REWARD3,OTHERS "
					+ "from QMS_REWARDS_FILE_OUTPUT where MEMBER_ID = '" + mid + "'");
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				rewardsFileOutputList.setRewardId(resultSet.getString("PERSONA_NAME"));
				rewardsFileOutputList.setMemberId(resultSet.getString("MEMBER_ID"));
				rewardsFileOutputList.setName(resultSet.getString("NAME"));
				rewardsFileOutputList.setAge(resultSet.getString("AGE"));
				rewardsFileOutputList.setGender(resultSet.getString("GENDER"));
				rewardsFileOutputList.setWeight(resultSet.getString("WEIGHT"));
				rewardsFileOutputList.setPersona(resultSet.getString("PERSONA"));
				rewardsFileOutputList.setPreferredReward(resultSet.getString("PREFERRED_REWARD"));
				rewardsFileOutputList.setMotivations(resultSet.getString("MOTIVATIONS"));
				rewardsFileOutputList.setCategory(resultSet.getString("CATEGORY"));
				rewardsFileOutputList.setGoal(resultSet.getString("GOAL"));
				rewardsFileOutputList.setFrequency(resultSet.getString("FREQUENCY"));
				rewardsFileOutputList.setGoalDate(resultSet.getString("GOAL_DATE"));
				rewardsFileOutputList.setReward1(resultSet.getString("REWARD1"));
				rewardsFileOutputList.setReward2(resultSet.getString("REWARD2"));
				rewardsFileOutputList.setReward3(resultSet.getString("REWARD3"));
				rewardsFileOutputList.setOthers(resultSet.getString("OTHERS"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return rewardsFileOutputList;
	}

	@Override
	public RestResult insertRewardsFileOutput(RewardsFileOutput rewardsFileOutput) {
		String sqlStatementInsert = "insert into QMS_REWARDS_FILE_OUTPUT"
										+ "( REWARD_ID,MEMBER_ID,NAME,AGE,GENDER,WEIGHT," 
										+ "PERSONA,PREFERRED_REWARD,MOTIVATIONS,CATEGORY,"
										+ "GOAL,FREQUENCY,GOAL_DATE,REWARD1,REWARD2,REWARD3) "
										+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement statement = null;
		Connection connection = null;
		ResultSet resultSet = null;
		Statement getStatement = null;
		User userData1 = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		try {
			connection = qmsConnection.getOracleConnection();
			// int factGoalRecId = 0;
			getStatement = connection.createStatement();
			// resultSet = getStatement.executeQuery("select
			// max(RECOMMENDATION_ID)from QMS_FACT_GOAL_RECOMMENDATIONS");
			// while (resultSet.next()) {
			// factGoalRecId = resultSet.getInt(1);
			// }
			// factGoalRecId = factGoalRecId + 1;
			int i = 0;
			Date date = new Date();
			Timestamp timestamp = new Timestamp(date.getTime());
			statement = connection.prepareStatement(sqlStatementInsert);
			// statement.setInt(++i, factGoalRecId);
			statement.setString(++i, rewardsFileOutput.getRewardId());
			statement.setString(++i, rewardsFileOutput.getMemberId());
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
			if (userData1 != null && userData1.getName() != null)
				statement.setString(++i, userData1.getName());
			else
				statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
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
	
}
