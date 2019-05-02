package com.qms.rest.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.LhcMemberView;
import com.qms.rest.model.LhrMemberListView;
import com.qms.rest.model.RewardSet;
import com.qms.rest.model.SMVMemberDetails;
import com.qms.rest.model.SMVMemberPayerClustering;
import com.qms.rest.model.SmvMember;
import com.qms.rest.model.SmvMemberClinical;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSDateUtil;

@Service("smvService")
public class SMVServiceImpl implements SMVService {

	@Autowired
	private QMSConnection qmsConnection;

	@Autowired
	private HttpSession httpSession;

	@Override
	public Set<SMVMemberDetails> getSMVMemberDetails(String memberId) {
		Set<SMVMemberDetails> memberDetailsList = new HashSet<>();
		SMVMemberDetails memberDetails = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		String query = null;
		try {
//			//oracle
//			connection = qmsConnection.getOracleConnection();
//			query = "select * from SMV_MEMBER_DETAILS_VIEW where MEMBER_ID='" + memberId + "'";
			
			//hive
			User userData = qmsConnection.getLoggedInUser();
			if(userData == null) {
				return null;
				//throw new QMSException(" Logged in user data is not available. Please logout and login again.");
			}
			System.out.println(" test 123 ");
			connection = qmsConnection.getHiveConnectionBySchemaName(null, userData.getLoginId(), userData.getPassword());			
			query = "select * from healthin.fact_smv_member_details where MEMBER_ID='" + memberId + "'";
			
			statement = connection.createStatement();
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				memberDetails = new SMVMemberDetails();
				memberDetails.setMemberId(resultSet.getString("MEMBER_ID"));
				memberDetails.setName(resultSet.getString("NAME"));
				memberDetails.setAddress(resultSet.getString("ADDRESS"));
				memberDetails.setPhone(resultSet.getString("PHONE"));
				memberDetails.setEmailAddress(resultSet.getString("EMAIL_ADDRESS"));
				memberDetails.setAge(resultSet.getString("AGE"));
				memberDetails.setGender(resultSet.getString("GENDER"));
				memberDetails.setEthnicity(resultSet.getString("ETHNICITY"));
				memberDetails.setIncome(resultSet.getString("INCOME"));
				memberDetails.setOccupation(resultSet.getString("OCCUPATION"));
				
				//oracle				
//				memberDetails.setPcpName(resultSet.getString("PCP NAME"));
//				memberDetails.setPcpNpi(resultSet.getString("PCP NPI"));
//				memberDetails.setPcpSpeciality(resultSet.getString("PCP SPECIALITY"));
//				memberDetails.setPcpAddress(resultSet.getString("PCP ADDRESS"));
//				memberDetails.setPhysicianName(resultSet.getString("PHYSICIAN NAME"));	
				
				//hive
				memberDetails.setPcpName(resultSet.getString("PCP_NAME"));
				memberDetails.setPcpNpi(resultSet.getString("PCP_NPI"));
				memberDetails.setPcpSpeciality(resultSet.getString("PCP_SPECIALITY"));
				memberDetails.setPcpAddress(resultSet.getString("PCP_ADDRESS"));
				memberDetails.setPhysicianName(resultSet.getString("PHYSICIAN_NAME"));	
								
				memberDetails.setNextAppointmentDate(resultSet.getString("NEXT_APPOINTMENT_DATE"));
				memberDetails.setDepartmentName(resultSet.getString("DEPARTMENT_NAME"));
				memberDetails.setNoShowLikelihood(resultSet.getString("NOSHOW_LIKELIHOOD"));
				memberDetails.setNoShow(resultSet.getString("NOSHOW"));
				String ssn = resultSet.getString("SSN");
				if(ssn != null && ssn.equalsIgnoreCase("Not Authorized"))
					memberDetails.setSsn("***-**-****");
				else
					memberDetails.setSsn(ssn);

				memberDetailsList.add(memberDetails);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return memberDetailsList;
	}

	@Override
	public Set<SmvMemberClinical> getSmvMemberClinical(String memberId) {
		Set<SmvMemberClinical> smvMemberClinicalSet = new HashSet<>();
		SmvMemberClinical smvMemberClinical = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			resultSet = statement
					.executeQuery("select * from SMV_MEMBER_CLINICAL_VIEW " + "where MEMBER_ID='" + memberId + "'");
			while (resultSet.next()) {
				smvMemberClinical = new SmvMemberClinical();
				smvMemberClinical.setProcedureName(resultSet.getString("PROCEDURE_NAME"));
				smvMemberClinical.setDrugCode(resultSet.getString("DRUG_CODE"));
				smvMemberClinical.setEncCsnId(resultSet.getString("ENC_CSN_ID"));
				smvMemberClinical.setMemberId(resultSet.getString("MEMBER_ID"));
				smvMemberClinical.setDrugCode(resultSet.getString("IMMUNIZATION_NAME"));
				smvMemberClinical.setImmunizationStatus(resultSet.getString("IMMUNIZATION_STATUS"));
				smvMemberClinical.setEncounterDateSk(resultSet.getString("ENCOUNTER_DATE_SK"));
				smvMemberClinicalSet.add(smvMemberClinical);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return smvMemberClinicalSet;
	}

	@Override
	public Set<SMVMemberPayerClustering> getSMVMemberPayerClustering(String memberId) {
		Set<SMVMemberPayerClustering> memberDetailsList = new HashSet<>();
		SMVMemberPayerClustering memberDetails = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select * from SMV_MEMBER_PAYER_CLUSTERING_VIEW where MEMBER_ID='" + memberId + "'";
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				memberDetails = new SMVMemberPayerClustering();
				memberDetails.setMemberId(resultSet.getString("MEMBER_ID"));
				memberDetails.setLob(resultSet.getString("LOB"));
				memberDetails.setCode(resultSet.getString("CODE"));
				memberDetails.setPlanName(resultSet.getString("PLAN_NAME"));
				memberDetails.setPlanCategory(resultSet.getString("PLAN_CATEGORY"));
				//memberDetails.setMemberPlanStartDateSk(resultSet.getString("MEMBER_PLAN_START_DATE_SK"));
				memberDetails.setMemberPlanEndDateSk(resultSet.getString("MEMBER_PLAN_END_DATE_SK"));
				memberDetails.setNoOfPendingClaimsYtd(resultSet.getString("NO_OF_PENDING_CLAIMS_YTD"));
				memberDetails.setNoOfDeniedClaimsYtd(resultSet.getString("NO_OF_DENIED_CLAIMS_YTD"));
				memberDetails.setAmountSpentYtd(resultSet.getString("AMOUNT_SPENT_YTD"));
				memberDetails.setPersonaName(resultSet.getString("PERSONA_NAME"));
				memberDetails.setPreferredGoal(resultSet.getString("PREFERRED_GOAL"));
				memberDetails.setPreferredReward(resultSet.getString("PREFERRED_REWARD"));
				memberDetails.setChannel(resultSet.getString("CHANNEL"));
				memberDetails.setLikelihoodEnrollment(resultSet.getString("LIKELIHOOD_ENROLLMENT"));
				memberDetails.setReasonToNotEnroll(resultSet.getString("REASON_TO_NOT_ENROLL"));
				memberDetailsList.add(memberDetails);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return memberDetailsList;
	}

	@Override
	public Set<LhcMemberView> getLhcMemberViewList() {
		Set<LhcMemberView> memberDetailsList = new HashSet<>();
		LhcMemberView lhcMemberDetails = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select * from  LHC_MEMBERLIST_VIEW";
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				lhcMemberDetails = new LhcMemberView();
				lhcMemberDetails.setMemberId(resultSet.getString("MEMBER_ID"));
				lhcMemberDetails.setName(resultSet.getString("NAME"));
				lhcMemberDetails.setAge(resultSet.getString("AGE"));
				lhcMemberDetails.setGender(resultSet.getString("GENDER"));
				lhcMemberDetails.setPhysicalActivityGoal(resultSet.getString("PHYSICAL_ACTIVITY_GOAL"));
				lhcMemberDetails.setPhysicalActivityFrequency(resultSet.getString("PHYSICAL_ACTIVITY_FREQUENCY"));
				lhcMemberDetails.setPhysicalActivityDuration(resultSet.getString("PHYSICAL_ACTIVITY_DURATION"));
				lhcMemberDetails.setCalorieIntakeGoal(resultSet.getString("CALORIE_INTAKE_GOAL"));
				lhcMemberDetails.setCalorieIntakeFrequency(resultSet.getString("CALORIE_INTAKE_FREQUENCY"));
				lhcMemberDetails.setCalorieIntakeDuration(resultSet.getString("CALORIE_INTAKE_DURATION"));
				lhcMemberDetails.setCareGap(resultSet.getString("CARE_GAP"));
				lhcMemberDetails.setCareGapDuration(resultSet.getString("CARE_GAP_DURATION"));
				lhcMemberDetails.setReward(resultSet.getString("REWARD"));
				lhcMemberDetails.setMotivations(resultSet.getString("MOTIVATIONS"));
				lhcMemberDetails.setLikelihoodToChurn(resultSet.getString("LIKELIHOOD_TO_CHURN"));
				lhcMemberDetails.setPredictedChurn(resultSet.getString("PREDICTED_CHURN"));				
				memberDetailsList.add(lhcMemberDetails);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return memberDetailsList;
	}

	public Set<LhrMemberListView> getLhrMemberListView() {
		Set<LhrMemberListView> lhrMemberList = new HashSet<>();
		LhrMemberListView lhrMemberListView = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select * from LHR_MEMBERLIST_VIEW";
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				lhrMemberListView = new LhrMemberListView();
				lhrMemberListView.setName(resultSet.getString("NAME"));
				lhrMemberListView.setMember_id(resultSet.getString("MEMBER_ID"));
				lhrMemberListView.setAge(resultSet.getString("AGE"));
				lhrMemberListView.setGender(resultSet.getString("GENDER"));
				lhrMemberListView.setPersona(resultSet.getString("PERSONA"));
				lhrMemberListView.setMotivations(resultSet.getString("MOTIVATIONS"));
				lhrMemberListView.setPhysicalActivityGoal(resultSet.getString("PHYSICAL_ACTIVITY_GOAL"));
				lhrMemberListView.setPhysicalActivityFrequency(resultSet.getString("PHYSICAL_ACTIVITY_FREQUENCY"));
				lhrMemberListView.setPhysicalActivityDuration(resultSet.getString("PHYSICAL_ACTIVITY_DURATION"));
				lhrMemberListView.setCalorieIintakeGoal(resultSet.getString("CALORIE_INTAKE_GOAL"));
				lhrMemberListView.setCalorieIntakeFrequency(resultSet.getString("CALORIE_INTAKE_FREQUENCY"));
				lhrMemberListView.setCalorieIntakeDuration(resultSet.getString("CALORIE_INTAKE_DURATION"));
				lhrMemberListView.setCareGap(resultSet.getString("CARE_GAP"));
				lhrMemberListView.setCareGapDuration(resultSet.getString("CARE_GAP_DURATION"));
				lhrMemberListView.setPerformancePhysicalActivity(resultSet.getString("PERFORMANCE_PHYSICAL_ACTIVITY"));
				lhrMemberListView.setPerformanceCalorieIntake(resultSet.getString("PERFORMANCE_CALORIE_INTAKE"));
				lhrMemberListView.setPerformanceCareGap(resultSet.getString("PERFORMANCE_CARE_GAP"));
				lhrMemberListView.setEducation(resultSet.getString("EDUCATION"));
				lhrMemberListView.setIncome(resultSet.getString("INCOME"));
				lhrMemberListView.setFamilySize(resultSet.getString("FAMILY_SIZE"));
				lhrMemberListView.setLikelihoodToRecommend(resultSet.getString("LIKELIHOOD_TO_RECOMMEND"));
				lhrMemberListView.setRecommendation(resultSet.getString("RECOMMENDATION"));
				lhrMemberList.add(lhrMemberListView);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return lhrMemberList;
	}
	
	@Override
    public Set<String> getMemberIdList(String memberListType) {
		Set<String> dataSet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;         
		Connection connection = null;
		try {    
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String sqlQuery = null;
			if(memberListType.equalsIgnoreCase("enrollment"))
				sqlQuery = "select DISTINCT A.MEMBER_ID FROM DIM_MEMBER A "
						+ "INNER JOIN QMS_ENROLLMENT_FILE_OUTPUT B ON A.MEMBER_ID = B.MEMBER_ID";
                if(memberListType.equalsIgnoreCase("goalsRecommendations"))
                	sqlQuery = "select  DISTINCT a.MEMBER_ID from DIM_MEMBER a "
                            + "INNER JOIN FACT_SURVEY b on b.MEMBER_SK =a.MEMBER_SK";      
                if(memberListType.equalsIgnoreCase("rewardsRecommendations"))
                	sqlQuery = "select DISTINCT MEMBER_ID from QMS_FACT_GOAL_INTERVENTIONS";
                resultSet = statement.executeQuery(sqlQuery);
                while (resultSet.next()) {
                	dataSet.add(resultSet.getString("MEMBER_ID"));              
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
	public SmvMember getSmvMember(String memberId) {
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		SmvMember smvMember = new SmvMember();
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String sqlQuery = null;

			// SMV INSURANCE
			sqlQuery = "SELECT DM.MEMBER_ID, COUNT(FC.CLAIMS_SK) AS \"Number_of_Pending_Claims\", MAX(DDS.CALENDAR_DATE)"
					+ " FROM FACT_MEMBERSHIP FM" + " INNER JOIN DIM_MEMBER DM ON FM.MEMBER_SK = DM.MEMBER_SK"
					+ " INNER JOIN FACT_CLAIMS FC ON FC.MEMBER_SK = FM.MEMBER_SK"
					+ " INNER JOIN DIM_DATE DD ON DD.DATE_SK = FC.START_DATE_SK"
					+ " INNER JOIN DIM_DATE DDS ON DDS.DATE_SK = FM.MEMBER_PLAN_START_DATE_SK"
					+ " INNER JOIN DIM_DATE DDE ON DDE.DATE_SK = FM.MEMBER_PLAN_END_DATE_SK"
					+ " WHERE DD.CALENDAR_DATE BETWEEN DDS.CALENDAR_DATE AND DDE.CALENDAR_DATE"
					+ " AND FC.CLAIM_STATUS= 'Pending' AND DM.MEMBER_ID='" + memberId + "'" + " GROUP BY DM.MEMBER_ID";
			resultSet = statement.executeQuery(sqlQuery);
			while (resultSet.next()) {
				smvMember.setMemberId(resultSet.getString("MEMBER_ID"));
				smvMember.setNumberOfPendingClaims(resultSet.getString("Number_of_Pending_Claims"));
				smvMember.setPendingClaimsDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate(3)));
			}
			resultSet.close();

			sqlQuery = "SELECT DM.MEMBER_ID, COUNT(FC.CLAIMS_SK) AS \"Number_of_Denied_Claims\", MAX(DDS.CALENDAR_DATE)"
					+ " FROM FACT_MEMBERSHIP FM" + " INNER JOIN DIM_MEMBER DM ON FM.MEMBER_SK = DM.MEMBER_SK"
					+ " INNER JOIN FACT_CLAIMS FC ON FC.MEMBER_SK = FM.MEMBER_SK"
					+ " INNER JOIN DIM_DATE DD ON DD.DATE_SK = FC.START_DATE_SK"
					+ " INNER JOIN DIM_DATE DDS ON DDS.DATE_SK = FM.MEMBER_PLAN_START_DATE_SK"
					+ " INNER JOIN DIM_DATE DDE ON DDE.DATE_SK = FM.MEMBER_PLAN_END_DATE_SK"
					+ " WHERE DD.CALENDAR_DATE BETWEEN DDS.CALENDAR_DATE AND DDE.CALENDAR_DATE"
					+ " AND FC.CLAIM_STATUS= 'Denied' AND DM.MEMBER_ID='" + memberId + "'" + " GROUP BY DM.MEMBER_ID";
			resultSet = statement.executeQuery(sqlQuery);
			while (resultSet.next()) {
				smvMember.setNumberOfDeniedClaims(resultSet.getString("Number_of_Denied_Claims"));
				smvMember.setDeniedClaimsDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate(3)));
			}
			resultSet.close();

			sqlQuery = "SELECT DM.MEMBER_ID, SUM(AMOUNT_PAID) AS \"Total_Amount_Spend\", MAX(DDS.CALENDAR_DATE)"
					+ " FROM FACT_MEMBERSHIP FM" + " INNER JOIN DIM_MEMBER DM ON FM.MEMBER_SK = DM.MEMBER_SK"
					+ " INNER JOIN FACT_CLAIMS FC ON FC.MEMBER_SK = FM.MEMBER_SK"
					+ " INNER JOIN DIM_DATE DD ON DD.DATE_SK = FC.START_DATE_SK"
					+ " INNER JOIN DIM_DATE DDS ON DDS.DATE_SK = FM.MEMBER_PLAN_START_DATE_SK"
					+ " INNER JOIN DIM_DATE DDE ON DDE.DATE_SK = FM.MEMBER_PLAN_END_DATE_SK"
					+ " WHERE DD.CALENDAR_DATE BETWEEN DDS.CALENDAR_DATE AND DDE.CALENDAR_DATE" + " AND DM.MEMBER_ID='"
					+ memberId + "'" + " GROUP BY DM.MEMBER_ID";
			resultSet = statement.executeQuery(sqlQuery);
			while (resultSet.next()) {
				smvMember.setTotalAmountSpend(resultSet.getString("Total_Amount_Spend"));
				smvMember.setAmountSpendDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate((3))));
			}
			resultSet.close();

			sqlQuery = "SELECT DISTINCT B.MEMBER_ID as \"Beneficiary_ID\", C.PAYER_NAME as \"Primary_Payer\","
					+ "RL.LOB, D.CODE AS \"Product\",D.PLAN_NAME AS \"Plan\","
					+ "D.PLAN_CATEGORY AS \"Plan_Type\",DD.CALENDAR_DATE AS \"Enrollment_Start_Date\","
					+ "DDT.CALENDAR_DATE AS \"Enrollment_End_Date\" FROM DIM_MEMBER B "
					+ "INNER JOIN FACT_MEMBERSHIP A ON A.MEMBER_SK = B.MEMBER_SK "
					+ "INNER JOIN FACT_CLAIMS FC ON FC.MEMBER_SK = A.MEMBER_SK "
					+ "INNER JOIN DIM_PAYER C ON A.PRIMARY_PAYER_SK = C.PAYER_SK "
					+ "INNER JOIN DIM_PRODUCT_PLAN D ON A.PRODUCT_PLAN_SK = D.PRODUCT_PLAN_SK "
					+ "INNER JOIN REF_LOB RL ON RL.LOB_ID = D.LOB_ID "
					+ "INNER JOIN DIM_DATE DD ON DD.DATE_SK = A.MEMBER_PLAN_START_DATE_SK "
					+ "INNER JOIN DIM_DATE DDT ON DDT.DATE_SK = A.MEMBER_PLAN_END_DATE_SK WHERE B.MEMBER_ID='"+memberId+"'";
			resultSet = statement.executeQuery(sqlQuery);
			while (resultSet.next()) {
				smvMember.setEnrollmentPrimaryPayer(resultSet.getString("Primary_Payer"));
				smvMember.setEnrollmentLob(resultSet.getString("lob"));
				smvMember.setEnrollmentProduct(resultSet.getString("Product"));
				smvMember.setEnrollmentPlan(resultSet.getString("Plan"));
				smvMember.setEnrollmentPlanType(resultSet.getString("Plan_Type"));
				smvMember.setEnrollmentStartDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("Enrollment_Start_Date")));
				smvMember.setEnrollmentEndDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("Enrollment_End_Date")));
			}
			resultSet.close();

			// SMV ENGAGEMENT
			sqlQuery = "SELECT CD.PERSONA_NAME, CFO.MEMBER_ID, EFO.LIKELIHOOD_ENROLLMENT,"
					+ "EFO.REASON_TO_NOT_ENROLL, EFO.CHANNEL, LHC.LIKELIHOOD_TO_CHURN,"
					+ "LHR.LIKELIHOOD_TO_RECOMMEND FROM QMS_CP_FILE_OUTPUT CFO "
					+ "INNER JOIN QMS_CP_DEFINE CD ON CD.CLUSTER_ID = CFO.CLUSTER_ID "
					+ "INNER JOIN QMS_ENROLLMENT_FILE_OUTPUT EFO ON EFO.MEMBER_ID = CFO.MEMBER_ID "
					+ "INNER JOIN QMS_LHC_FILE_OUTPUT LHC ON LHC.MEMBER_ID = EFO.MEMBER_ID "
					+ "INNER JOIN QMS_LHR_FILE_OUTPUT LHR ON LHR.MEMBER_ID = EFO.MEMBER_ID WHERE CFO.MEMBER_ID='"+memberId+"'";
			resultSet = statement.executeQuery(sqlQuery);
			while (resultSet.next()) {
				smvMember.setEngagementPersonaName(resultSet.getString("PERSONA_NAME"));
				smvMember.setEngagementLikelihoodEnrollment(resultSet.getString("LIKELIHOOD_ENROLLMENT"));
				smvMember.setEngagementReasonToNotEnroll(resultSet.getString("REASON_TO_NOT_ENROLL"));
				smvMember.setEngagementChannel(resultSet.getString("CHANNEL"));
				smvMember.setEngagementLikelihoodOfEnrollment(resultSet.getString("LIKELIHOOD_TO_CHURN"));
				smvMember.setEngagementLikelihoodToRecommend(resultSet.getString("LIKELIHOOD_TO_RECOMMEND"));
			}
			resultSet.close();
			
			//Goals & Rewards
			
			sqlQuery = "SELECT DISTINCT A.MEMBER_ID, A.GOAL_SET_ID, A.CATEGORY, A.GOAL, A.FREQUENCY, A.GOAL_DATE, A.REWARD, B.PERFORMANCE, C.INTERVENTIONS FROM QMS_REWARDS_SET A INNER JOIN QMS_FACT_PERFORMANCE B ON A.MEMBER_ID = B.MEMBER_ID AND A.GOAL_SET_ID = B.GOAL_SET_ID AND A.CATEGORY = B.CATEGORY INNER JOIN QMS_FACT_INTERVENTIONS C ON C.MEMBER_ID = B.MEMBER_ID AND C.GOAL_SET_ID = B.GOAL_SET_ID AND C.CATEGORY = B.CATEGORY WHERE A.MEMBER_ID='"+memberId+"'";

			/*sqlQuery = "SELECT DISTINCT A.MEMBER_ID, A.GOAL_SET_ID, A.CATEGORY, A.GOAL, "
					+ "A.FREQUENCY, A.GOAL_DATE,"
					+ "A.REWARD, B.PERFORMANCE, C.INTERVENTIONS FROM QMS_REWARDS_SET A "
					+ "INNER JOIN QMS_FACT_PERFORMANCE B ON A.MEMBER_ID = B.MEMBER_ID "
					+ "INNER JOIN QMS_FACT_INTERVENTIONS C ON C.MEMBER_ID = A.MEMBER_ID "
					+ "WHERE A.MEMBER_ID='"+memberId+"'";        */  
			resultSet = statement.executeQuery(sqlQuery);
			Set<RewardSet> rewardSetList = new HashSet<>();
			smvMember.setRewardSetList(rewardSetList);
			RewardSet rewardSet = null;
			while (resultSet.next()) {
				rewardSet = new RewardSet();
				rewardSet.setCategory(resultSet.getString("CATEGORY"));
				rewardSet.setGoal(resultSet.getString("GOAL"));
				rewardSet.setFrequency(resultSet.getString("FREQUENCY"));
				rewardSet.setGoalDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("GOAL_DATE")));
				rewardSet.setReward(resultSet.getString("REWARD"));
				rewardSet.setInterventions(resultSet.getString("INTERVENTIONS"));
				rewardSet.setPerformance(resultSet.getString("PERFORMANCE"));
				rewardSetList.add(rewardSet);
			}
			resultSet.close();

			sqlQuery = "SELECT DM.MEMBER_ID, MAX(DD.CALENDAR_DATE), FI.IMMUNIZATION_NAME AS \"Active_Immunizations\", FI.IMMUNIZATION_STATUS"
					+ " FROM DIM_MEMBER DM INNER JOIN FACT_PAT_MEM FPM ON FPM.MEMBER_ID = DM.MEMBER_ID"
					+ " INNER JOIN DIM_PATIENT DP ON DP.PAT_ID = FPM.PAT_ID"
					+ " INNER JOIN FACT_ENC FE ON FE.PATIENT_SK = DP.PATIENT_SK"
					+ " INNER JOIN FACT_IMMUNIZATION FI ON DP.PATIENT_SK = FI.PATIENT_SK"
					+ " INNER JOIN DIM_DATE DD ON DD.DATE_SK = FI.IMMUNIZATION_DATE_SK WHERE DM.MEMBER_ID ='"
					+ memberId + "' AND FI.IMMUNIZATION_STATUS = 'completed'"
					+ " GROUP BY DM.MEMBER_ID, FI.IMMUNIZATION_NAME, FI.IMMUNIZATION_STATUS";
			resultSet = statement.executeQuery(sqlQuery);
			while (resultSet.next()) {
				smvMember.setActiveImmunizationsDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate(2)));
				smvMember.setActiveImmunizations(resultSet.getString("Active_Immunizations"));
			}
			resultSet.close();

			sqlQuery = "SELECT DM.MEMBER_ID, MAX(DD.CALENDAR_DATE), FI.IMMUNIZATION_NAME AS \"Pending_Immunizations\", FI.IMMUNIZATION_STATUS"
					+ " FROM DIM_MEMBER DM INNER JOIN FACT_PAT_MEM FPM ON FPM.MEMBER_ID = DM.MEMBER_ID"
					+ " INNER JOIN DIM_PATIENT DP ON DP.PAT_ID = FPM.PAT_ID"
					+ " INNER JOIN FACT_ENC FE ON FE.PATIENT_SK = DP.PATIENT_SK"
					+ " INNER JOIN FACT_IMMUNIZATION FI ON DP.PATIENT_SK = FI.PATIENT_SK"
					+ " INNER JOIN DIM_DATE DD ON DD.DATE_SK = FI.IMMUNIZATION_DATE_SK WHERE DM.MEMBER_ID = '"
					+ memberId + "' AND FI.IMMUNIZATION_STATUS = 'pending'"
					+ " GROUP BY DM.MEMBER_ID, FI.IMMUNIZATION_NAME, FI.IMMUNIZATION_STATUS";
			resultSet = statement.executeQuery(sqlQuery);
			while (resultSet.next()) {
				smvMember.setPendingImmunizationsDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate(2)));
				smvMember.setPendingImmunizations(resultSet.getString("Pending_Immunizations"));
				smvMember.setPendingImmunizationsStatus(resultSet.getString("IMMUNIZATION_STATUS"));
			}
			resultSet.close();

			sqlQuery = "SELECT DM.MEMBER_ID, MAX(DD.CALENDAR_DATE), FOP.PROCEDURE_NAME AS \"Last_Prescribed_Procedures\""
					+ " FROM DIM_MEMBER DM" + " INNER JOIN FACT_PAT_MEM FPM ON FPM.MEMBER_ID = DM.MEMBER_ID"
					+ " INNER JOIN DIM_PATIENT DP ON DP.PAT_ID = FPM.PAT_ID"
					+ " INNER JOIN FACT_ENC FE ON FE.PATIENT_SK = DP.PATIENT_SK"
					+ " INNER JOIN FACT_ORDER_PROC FOP ON FOP.ENC_CSN_ID = FE.ENC_CSN_ID"
					+ " INNER JOIN DIM_DATE DD ON DD.DATE_SK = FOP.ORDER_DATE_SK" + " WHERE DM.MEMBER_ID = '" + memberId
					+ "'" + " GROUP BY DM.MEMBER_ID, FOP.PROCEDURE_NAME";
			resultSet = statement.executeQuery(sqlQuery);
			while (resultSet.next()) {
				smvMember.setLastPrescribedProceduresDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate(2)));
				smvMember.setLastPrescribedProcedures(resultSet.getString("Last_Prescribed_Procedures"));
			}
			resultSet.close();

			sqlQuery = "SELECT DM.MEMBER_ID, MAX(DD.CALENDAR_DATE), DME.MEDICATION_NAME AS \"Last_Prescribed_Medications\""
					+ " FROM DIM_MEMBER DM INNER JOIN FACT_PAT_MEM FPM ON FPM.MEMBER_ID = DM.MEMBER_ID"
					+ " INNER JOIN DIM_PATIENT DP ON DP.PAT_ID = FPM.PAT_ID"
					+ " INNER JOIN FACT_ENC FE ON FE.PATIENT_SK = DP.PATIENT_SK"
					+ " INNER JOIN FACT_ORDER_MED FOM ON FOM.ENC_CSN_ID = FE.ENC_CSN_ID"
					+ " INNER JOIN DIM_MEDICATION DME ON DME.NDC_CODE = FOM.NDC"
					+ " INNER JOIN DIM_DATE DD ON DD.DATE_SK = FOM.ORDER_DATE_SK WHERE DM.MEMBER_ID = '" + memberId+"'"
					+ " GROUP BY DM.MEMBER_ID, DME.MEDICATION_NAME";
			resultSet = statement.executeQuery(sqlQuery);
			while (resultSet.next()) {
				smvMember.setLastPrescribedMedicationsDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate(2)));
				smvMember.setLastPrescribedMedications(resultSet.getString("Last_Prescribed_Medications"));
			}
			resultSet.close();

			sqlQuery = "SELECT MAX(CALENDAR_DATE) AS \"Last_Date_of_Service\", FCRS.CCI_SCORE AS \"Risk\""
					+ " FROM DIM_MEMBER DM INNER JOIN FACT_PAT_MEM FPM ON FPM.MEMBER_ID = DM.MEMBER_ID"
					+ " INNER JOIN DIM_PATIENT DP ON DP.PAT_ID = FPM.PAT_ID"
					+ " INNER JOIN FACT_ENC FE ON FE.PATIENT_SK = DP.PATIENT_SK"
					+ " INNER JOIN DIM_DATE DD ON DD.DATE_SK = FE.ENCOUNTER_DATE_SK"
					+ " INNER JOIN FACT_CCI_RISK_SCORE FCRS ON DM.MEMBER_SK = FCRS.MEMBER_SK"
					+ " INNER JOIN QMS_GIC_LIFECYCLE QGL ON DM.MEMBER_ID = QGL.MEMBER_ID WHERE DM.MEMBER_ID = '"
					+ memberId + "'" + " GROUP BY FCRS.CCI_SCORE";
			resultSet = statement.executeQuery(sqlQuery);
			while (resultSet.next()) {
				smvMember.setLastDateOfService(QMSDateUtil.getSQLDateFormat(resultSet.getDate("Last_Date_of_Service")));
				smvMember.setRisk(resultSet.getString("Risk"));
			}
			resultSet.close();
			
			sqlQuery = "SELECT COUNT(IP.MEMBER_SK) AS \"IP_VISITS\" FROM IP_VISIT_VIEW IP WHERE IP.MEMBER_SK LIKE '"
					+ memberId + "%' UNION SELECT COUNT(OP.MEMBER_SK) AS \"IP_VISITS\" FROM OP_VISIT_VIEW OP"
					+ " WHERE OP.MEMBER_SK LIKE '" + memberId + "%' UNION"
					+ " SELECT COUNT(ER.MEMBER_SK) AS \"IP_VISITS\" FROM ER_FLYER_VIEW ER WHERE ER.MEMBER_SK LIKE '"+memberId+"%'";
			resultSet = statement.executeQuery(sqlQuery);
			int count = 1;
			while (resultSet.next()) {
				if(count == 1)
					smvMember.setIpVisits(resultSet.getString("IP_VISITS"));
				if(count == 2)
					smvMember.setOpVisits(resultSet.getString("IP_VISITS"));
				if(count == 3)
					smvMember.setErVisits(resultSet.getString("IP_VISITS"));
				count++;
			}
			
			//Comorbidities
			resultSet.close();
			sqlQuery = "select fmc.* from fact_mem_comorbidity fmc, dim_member dm where "
					+ "fmc.member_sk = dm.member_sk and dm.member_id = '"+memberId+"'";
			resultSet = statement.executeQuery(sqlQuery);
			Set<String> comorbidities = new TreeSet<>();
			ResultSetMetaData rsmd = resultSet.getMetaData();
			String colName = null;
			String colValue = null;
			while (resultSet.next()) {
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					colName = rsmd.getColumnName(i); 
					colValue = resultSet.getString(i);
					if(colValue != null && colValue.equalsIgnoreCase("1") && !colName.equalsIgnoreCase("fmc.comorbidity_count"))
						comorbidities.add(colName.replaceFirst("fmc.", "").toLowerCase());
                }
				break;
			}
			smvMember.setComorbidities(comorbidities);
			smvMember.setComorbiditiesCount(comorbidities.size()+"");
			
			//Care Gaps			
			resultSet.close();
			sqlQuery = "select dqm.measure_name, qgl.status, qgl.COMPLIANCE_POTENTIAL, qgl.gap_date "+
			"from qms_quality_measure dqm "+
			"inner join qms_gic_lifecycle qgl on dqm.quality_measure_id = qgl.quality_measure_id "+
			"where qgl.status <> 'closed' and qgl.member_id = '"+memberId+"' and rownum < 2 order by qgl.gap_date desc";			
			Set<String[]> careGaps = new HashSet<>();
			resultSet = statement.executeQuery(sqlQuery);
			Set<String> careGapNameSet = new HashSet<>();
			String gapName = null;
			while (resultSet.next()) {
				gapName = resultSet.getString("measure_name");
				if(!careGapNameSet.contains(gapName)) {					
					careGapNameSet.add(gapName);
					careGaps.add(new String[]{gapName, resultSet.getString("COMPLIANCE_POTENTIAL")});
				}
			}
			smvMember.setCareGaps(careGaps);
			smvMember.setCareGapsCount(careGaps.size()+"");			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return smvMember;
	}

	@Override
	public Set<RewardSet> getIntervention(String memberId) {
		Set<RewardSet> rewardSetList = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String sqlQuery = "select * from QMS_FACT_INTERVENTIONS where MEMBER_ID='" + memberId + "'";
			resultSet = statement.executeQuery(sqlQuery);
			RewardSet rewardSet = null;
			while (resultSet.next()) {
				rewardSet = new RewardSet();
				rewardSet.setCategory(resultSet.getString("CATEGORY"));
				rewardSet.setGoal(resultSet.getString("GOAL"));
				rewardSet.setFrequency(resultSet.getString("FREQUENCY"));
				rewardSet.setGoalDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("GOAL_DATE")));
				rewardSet.setGoalSetId(resultSet.getInt("GOAL_SET_ID"));
				rewardSet.setInterventions(resultSet.getString("INTERVENTIONS"));
				rewardSetList.add(rewardSet);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return rewardSetList;
	}	
}
