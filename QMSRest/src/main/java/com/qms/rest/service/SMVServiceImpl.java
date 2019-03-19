package com.qms.rest.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.LhcMemberView;
import com.qms.rest.model.LhrMemberListView;
import com.qms.rest.model.SMVMemberDetails;
import com.qms.rest.model.SMVMemberPayerClustering;
import com.qms.rest.model.SmvMemberClinical;
import com.qms.rest.util.QMSConnection;

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
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "select * from SMV_MEMBER_DETAILS_VIEW where MEMBER_ID='" + memberId + "'";
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
				memberDetails.setPcpName(resultSet.getString("PCP NAME"));
				memberDetails.setPcpNpi(resultSet.getString("PCP NPI"));
				memberDetails.setPcpSpeciality(resultSet.getString("PCP SPECIALITY"));
				memberDetails.setPcpAddress(resultSet.getString("PCP ADDRESS"));
				memberDetails.setNextAppointmentDate(resultSet.getString("NEXT_APPOINTMENT_DATE"));
				memberDetails.setDepartmentName(resultSet.getString("DEPARTMENT_NAME"));
				memberDetails.setNoShowLikelihood(resultSet.getString("NOSHOW_LIKELIHOOD"));
				memberDetails.setNoShow(resultSet.getString("NOSHOW"));
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
				memberDetails.setMemberPlanStartDateSk(resultSet.getString("MEMBER_PLAN_START_DATE_SK"));
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
			String query = "select * from LHR_MEMBERLIST_VIEW ";
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
	
	
	
	

}
