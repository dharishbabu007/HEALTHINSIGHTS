package com.qms.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.ChartAbs;
import com.qms.rest.model.DimMemberGapListSearch;
import com.qms.rest.model.Patient;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;
import com.qms.rest.util.QMSDateUtil;

@Service("chartAbstractionService")
public class ChartAbstractionServiceImpl implements ChartAbstractionService{
	
	@Autowired 
	private HttpSession httpSession;	
	
	@Autowired
	private QMSConnection qmsConnection;

	@Override
	public Patient getPatientDetails(String patientId) {
		Patient patient = null;
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select dp.*, dd.CALENDAR_DATE from dim_patient dp,"
					+ "dim_date dd where dd.date_sk = dp.DATE_OF_BIRTH_SK and PAT_ID='"+patientId+"'");
			while (resultSet.next()) {
				patient = new Patient();
				patient.setDob(QMSDateUtil.getSQLDateFormat(resultSet.getDate("CALENDAR_DATE")));
				patient.setFirstName(resultSet.getString("PAT_FIRST_NAME"));
				patient.setGender(resultSet.getString("GENDER"));
				patient.setLastName(resultSet.getString("PAT_LAST_NAME"));
				patient.setPatientId(resultSet.getString("PAT_ID"));
			}
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		
		return patient;
	}
	
	@Override
	public List<DimMemberGapListSearch> findSearchPatientList(String search) {
		List<DimMemberGapListSearch> dimMemberDetailLst = new ArrayList<DimMemberGapListSearch>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			
			String membergaptotallistqry = 
			"SELECT  DM.PAT_ID, DM.PAT_FIRST_NAME, DM.PAT_MIDDLE_NAME ,DM.PAT_LAST_NAME "
			+ "FROM DIM_PATIENT DM where DM.PAT_ID like '"+search+"%' or DM.PAT_FIRST_NAME like '"+search+"%' or DM.PAT_MIDDLE_NAME "
			+ "like '"+search+"%' or DM.PAT_LAST_NAME like '"+search+"%' order by DM.PAT_ID ASC";
			resultSet = statement.executeQuery(membergaptotallistqry);
			DimMemberGapListSearch dimMemberDetails = null;
			while (resultSet.next()) {
				dimMemberDetails = new DimMemberGapListSearch();
				dimMemberDetails.setFirstName(resultSet.getString("PAT_FIRST_NAME"));
				dimMemberDetails.setMiddelName(resultSet.getString("PAT_MIDDLE_NAME"));
				dimMemberDetails.setLastName(resultSet.getString("PAT_LAST_NAME"));
				dimMemberDetails.setMemberId(resultSet.getString("PAT_ID"));
				dimMemberDetailLst.add(dimMemberDetails);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return dimMemberDetailLst;
	}
	
	@Override
	public List<ChartAbs> getChartAbsVisits(String patientId) {
		List<ChartAbs> setOutput = null;
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from Chart_Abs where MEMBER_ID="+patientId+" and CHART_TYPE='VISIT' ORDER BY REC_CREATE_DATE");
			setOutput = populateChartAbsObject (resultSet);
			System.out.println(patientId + " getChartAbsVisits "+ setOutput.size());
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		return setOutput;
	}	

	@Override
	public List<ChartAbs> getChartAbs(String patientId, String chartType, int visitId) {
		List<ChartAbs> setOutput = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from Chart_Abs where MEMBER_ID="+patientId+" and VISIT="+ visitId +" and CHART_TYPE='"+chartType+"' ORDER BY REC_CREATE_DATE");
			setOutput = populateChartAbsObject (resultSet);
			if(setOutput == null || setOutput.isEmpty()) {
				ChartAbs chartAbs = new ChartAbs();
				chartAbs.setMemberId(Integer.parseInt(patientId));
				chartAbs.setChartType(chartType);
				setOutput = new ArrayList<>();
				setOutput.add(chartAbs);
				return setOutput;
			}			
			System.out.println(patientId + ", " + chartType + ", " +visitId + " getChartAbs "+ setOutput.size());
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		return setOutput;
	}
	
	@Override
	public List<ChartAbs> getChartAbsHistory(String patientId, String chartType) {
		List<ChartAbs> setOutput = null;
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from Chart_Abs where MEMBER_ID="+patientId+" and CHART_TYPE='"+chartType+"' ORDER BY REC_CREATE_DATE");
			setOutput = populateChartAbsObject (resultSet);
			System.out.println(patientId + ", " + chartType + " getChartAbsHistory "+ setOutput.size());			
		} catch (Exception e) {
			e.printStackTrace();			
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		return setOutput;
	}	
	
	
	@Override
	public RestResult addChartAbs(List<ChartAbs> chartAbsList) {
		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = null; 
		Statement statementObj = null;
		ResultSet resultSet = null;
		try {		
			User userData = qmsConnection.getLoggedInUser();
			String valueSets = null;
			
			if(chartAbsList == null || chartAbsList.isEmpty())
				return RestResult.getFailRestResult(" Request body is null. ");
//			if(chartType == null || chartType.isEmpty())
//				return RestResult.getFailRestResult(" ChartType should not be null. ");	
			int patientId = chartAbsList.get(0).getMemberId();
			if(patientId == 0)
				return RestResult.getFailRestResult(" memberId is missing in request body. ");
			Patient patient = getPatientDetails(patientId+"");
			if(patient == null) {
				return RestResult.getFailRestResult(" Member id does not exists in DB. ");
			}			
			if(chartAbsList.get(0).getChartType().equalsIgnoreCase("VISIT")) {
				chartAbsList.get(0).setFirstName(patient.getFirstName());
				chartAbsList.get(0).setLastName(patient.getLastName());
				chartAbsList.get(0).setDob(patient.getLastName());
				chartAbsList.get(0).setGender(patient.getGender());
				valueSets = getEncounterValueSets(chartAbsList.get(0).getEncounterCode());
			}			
			connection = qmsConnection.getOracleConnection();
			statementObj = connection.createStatement();
			
			//to get visitId
			int visitId = 0;
			if(chartAbsList.get(0) != null && chartAbsList.get(0).getVisit() == 0) {
				if(chartAbsList.get(0).getChartType() == null)
					return RestResult.getFailRestResult(" Chart type is null in one of the request object. ");
				if(chartAbsList.get(0).getChartType().equalsIgnoreCase("VISIT")) {
					resultSet = statementObj.executeQuery("select max(VISIT) from Chart_Abs where CHART_TYPE='VISIT' and MEMBER_ID="+patientId);
					if (resultSet.next()) {
						visitId = resultSet.getInt(1);
					}
					visitId = visitId+1;
					httpSession.setAttribute("VISIT_ID", visitId);
					resultSet.close();
				} else {
					visitId = (int) httpSession.getAttribute("VISIT_ID"); 
				}
			} else {
				visitId = chartAbsList.get(0).getVisit();
			}
			
			//to get visitLineId
			int visitLineId = 0;			
			resultSet = statementObj.executeQuery("select max(VISIT_LINE_ID) from Chart_Abs where VISIT="+visitId+" and MEMBER_ID="+patientId);
			if (resultSet.next()) {
				visitLineId = resultSet.getInt(1);
			}
			visitLineId = visitLineId+1;
			
			String sqlStatementInsert = "insert into Chart_Abs(MEMBER_ID,VISIT,VISIT_LINE_ID,FIRST_NAME,"
					+ "LAST_NAME,DOB,GENDER,PROVIDER,ENCOUNTER_START_DATE,ENCOUNTER_END_DATE,ENCOUNTER_CODE,"
					+ "ENCOUNTER_VALUESET,ALLERGIES_DRUG,ALLERGIES_DRUG_CODE,ALLERGIES_REACTION,"
					+ "ALLERGIES_START_DATE,ALLERGIES_END_DATE,ALLERGIES_NOTES,ALLERGIES_OTHERS,"
					+ "ALLERGIES_OTHERS_CODE,ALLERGIES_OTHERS_REACTION,ALLERGIES_OTHERS_START_DATE,"
					+ "ALLERGIES_OTHERS_END_DATE,ALLERGIES_OTHERS_NOTES,MEDICATIONS_NDC_DRUG_CODE_NAME,"
					+ "MEDICATIONS_DRUG_CODE,MEDICATIONS_DAYS_SUPPLIED,MEDICATIONS_QUANTITY,"
					+ "MEDICATIONS_NOTES,MEDICATIONS_PRESCRIPTION_DATE,MEDICATIONS_START_DATE,"
					+ "MEDICATIONS_DISPENSED_DATE,MEDICATIONS_END_DATE,MEDICATIONS_PROVIDER_NPI,"
					+ "MEDICATIONS_PHARMACY_NPI,IMMUNIZATIONS_NAME,IMMUNIZATIONS_CODE,"
					+ "IMMUNIZATIONS_START_DATE,IMMUNIZATIONS_END_DATE,IMMUNIZATIONS_NOTES,"
					+ "DISEASE,DISEASE_CODE,DISEASE_DIAGNOSIS_DATE,DISEASE_END_DATE,DISEASE_NOTES,"
					+ "SDOH_CATEGORY,SDOH_CATEGORY_CODE,SDOH_PROBLEM,SDOH_PROBLEM_CODE,SDOH_NOTES,"
					+ "SUBSTANCE_USE,SUBSTANCE_USE_CODE,SUBSTANCE_FREQUENCY,SUBSTANCE_NOTES,"
					+ "FAMILY_HISTORY_RELATION,FAMILY_HISTORY_STATUS,FAMILY_HISTORY_DISEASE,"
					+ "FAMILY_HISTORY_DISEASE_CODE,FAMILY_HISTORY_NOTES,CHART_TYPE,PROV_ID,"
					+ "CURR_FLAG,REC_CREATE_DATE,REC_UPDATE_DATE,LATEST_FLAG,"
					+ "ACTIVE_FLAG,INGESTION_DATE,SOURCE_NAME,USER_NAME) "
					+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
					+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			
			statement = connection.prepareStatement(sqlStatementInsert);
			for (ChartAbs chartAbs : chartAbsList) {
				System.out.println(" Adding Chart_Abs with chartType,visitId,visitLineId,patientId "+chartAbs.getChartType()+", "+visitId+", "+visitLineId+", "+patientId);
				System.out.println(chartAbs.getVisit()+ " VISIT INFO " + visitId);
				if(chartAbs.getChartType() == null || chartAbs.getChartType().isEmpty()) {
					return RestResult.getFailRestResult(" Chart type is null in one of the request object. ");
				}
				int i=0; 
		    	statement.setInt(++i, chartAbs.getMemberId());
		    	statement.setInt(++i, visitId);
		    	statement.setInt(++i, visitLineId);
		    	statement.setString(++i, chartAbs.getFirstName());
		    	statement.setString(++i, chartAbs.getLastName());
		    	statement.setString(++i, chartAbs.getDob());
		    	statement.setString(++i, chartAbs.getGender());
		    	statement.setString(++i, chartAbs.getProvider());
		    	statement.setString(++i, chartAbs.getEncounterStartDate());
		    	statement.setString(++i, chartAbs.getEncounterEndDate());
		    	statement.setString(++i, getStringFromList(chartAbs.getEncounterCode()));
		    	statement.setString(++i, valueSets);
		    	
		    	//tabs begin
		    	statement.setString(++i, chartAbs.getAllergiesDrug());
		    	statement.setString(++i, chartAbs.getAllergiesDrugCode());
		    	statement.setString(++i, chartAbs.getAllergiesReaction());
		    	statement.setString(++i, chartAbs.getAllergiesStartDate());
		    	statement.setString(++i, chartAbs.getAllergiesEndDate());		    	
		    	statement.setString(++i, chartAbs.getAllergiesNotes());
		    	statement.setString(++i, chartAbs.getAllergiesOthers());
		    	statement.setString(++i, chartAbs.getAllergiesOthersCode());
		    	statement.setString(++i, chartAbs.getAllergiesOthersReaction());
		    	statement.setString(++i, chartAbs.getAllergiesOthersStartDate());
		    	statement.setString(++i, chartAbs.getAllergiesOthersEndDate());		    	
		    	statement.setString(++i, chartAbs.getAllergies_others_notes());
		    	statement.setString(++i, chartAbs.getMedicationsNdcDrugCodeName());
		    	statement.setString(++i, chartAbs.getMedicationsDrugCode());
		    	statement.setString(++i, chartAbs.getMedicationsDaysSupplied());
		    	statement.setString(++i, chartAbs.getMedicationsQuantity());
		    	statement.setString(++i, chartAbs.getMedicationsNotes());
		    	statement.setString(++i, chartAbs.getMedicationsPrescriptionDate());
		    	statement.setString(++i, chartAbs.getMedicationsStartDate());
		    	statement.setString(++i, chartAbs.getMedicationsDispensedDate());		    	
		    	statement.setString(++i, chartAbs.getMedicationsEndDate());
		    	statement.setString(++i, chartAbs.getMedicationsProviderNpi());
		    	statement.setString(++i, chartAbs.getMedicationsPharmacyNpi());
		    	statement.setString(++i, chartAbs.getImmunizationsName());
		    	statement.setString(++i, chartAbs.getImmunizationsCode());
		    	statement.setString(++i, chartAbs.getImmunizationsStartDate());
		    	statement.setString(++i, chartAbs.getImmunizationsEndDate());
		    	statement.setString(++i, chartAbs.getImmunizationsNotes());
		    	statement.setString(++i, chartAbs.getDisease());
		    	statement.setString(++i, chartAbs.getDiseaseCode());
		    	statement.setString(++i, chartAbs.getDiseaseDiagnosisDate());
		    	statement.setString(++i, chartAbs.getDiseaseEndDate());
		    	statement.setString(++i, chartAbs.getDiseaseNotes());
		    	statement.setString(++i, chartAbs.getSdohCategory());
		    	statement.setString(++i, chartAbs.getSdohCategoryCode());
		    	statement.setString(++i, chartAbs.getSdohProblem());
		    	statement.setString(++i, chartAbs.getSdohProblemCode());
		    	statement.setString(++i, chartAbs.getSdohNotes());
		    	statement.setString(++i, chartAbs.getSubstanceUse());
		    	statement.setString(++i, chartAbs.getSubstanceUseCode());
		    	statement.setString(++i, chartAbs.getSubstanceFrequency());
		    	statement.setString(++i, chartAbs.getSubstanceNotes());
		    	statement.setString(++i, chartAbs.getFamilyHistoryRelation());
		    	statement.setString(++i, chartAbs.getFamilyHistoryStatus());
		    	statement.setString(++i, chartAbs.getFamilyHistoryDisease());
		    	statement.setString(++i, chartAbs.getFamilyHistoryDiseaseCode());
		    	statement.setString(++i, chartAbs.getFamilyHistoryNotes());
		    	statement.setString(++i, chartAbs.getChartType());
		    	statement.setString(++i, chartAbs.getProvId());
		    	//tabs end
				
				Date date = new Date();				
				Timestamp timestamp = new Timestamp(date.getTime());				
				statement.setString(++i, "Y");
				statement.setTimestamp(++i, timestamp);
				statement.setTimestamp(++i, timestamp);
				statement.setString(++i, "Y");
				statement.setString(++i, "A");
				statement.setTimestamp(++i, timestamp);
				statement.setString(++i, QMSConstants.MEASURE_SOURCE_NAME);				
				if(userData == null || userData.getId() == null)
					statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
				else
					statement.setString(++i, userData.getLoginId());
				statement.addBatch();
				visitLineId = visitLineId+1;
			}
			statement.executeBatch();
			restResult = RestResult.getSucessRestResult("ChartAbs added successfully.");
		} catch (Exception e) {
			e.printStackTrace();
			restResult = RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statementObj, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}
		return restResult;		
	}

	@Override
	public Set<String> getEncounterTypes() {
		Set<String> dataSet = new TreeSet<>();
		Statement statement = null;
		ResultSet resultSet = null;         
		Connection connection = null;
		try {    
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String sqlQuery = "select CODESYSTEM, CODE, VALUESET from Ref_HEDIS2019 order by CODESYSTEM, CODE, VALUESET";
            resultSet = statement.executeQuery(sqlQuery);
            while (resultSet.next()) {
                dataSet.add(resultSet.getString("CODESYSTEM")+":"+resultSet.getString("CODE")
                +"   "+resultSet.getString("VALUESET"));
                if(dataSet.size() == 50)
                	break;
            }
		} catch (Exception e) {
			e.printStackTrace();
        }
        finally {
        	qmsConnection.closeJDBCResources(resultSet, statement, connection);
        }           
		return dataSet;               
    }	
	
	
	private String getEncounterValueSets(List<String> encounterCodeList) {
		if(encounterCodeList == null || encounterCodeList.isEmpty()) return "";
		Set<String> dataSet = new TreeSet<>();
		for (String code : encounterCodeList) {
			String[] codesAry = code.split(":");
			if(codesAry.length < 2) continue;
			String[] codesAry1 = codesAry[1].split("   ");
			if(codesAry1.length < 2) continue;
			dataSet.add(codesAry1[1]);
		}		
		System.out.println(encounterCodeList.toString() + " returned EncounterValueSets " + dataSet.toString());
		if(!dataSet.isEmpty())
			return dataSet.toString().substring(1, dataSet.toString().length()-1);
        return "";       
    }		
	
	
	private List<String> getListFromString (String value) {
		if(value == null)
			return new ArrayList<String>();
		value = value.replaceAll(", ", ",");
		List<String> valueList = Arrays.asList(value.split(","));
		return valueList;
	}

	private String getStringFromList (List<String> values) {
		if(values == null || values.isEmpty())
			return null;
		return values.toString().substring(1, values.toString().length()-1);
	}
	
	private List<ChartAbs> populateChartAbsObject (ResultSet resultSet) throws Exception {
		List<ChartAbs> setOutput = new ArrayList<>();
		ChartAbs output = null;
		while (resultSet.next()) {
	    	output = new ChartAbs();
	    	
	    	output.setMemberId(resultSet.getInt("MEMBER_ID"));
	    	output.setVisit(resultSet.getInt("VISIT"));
	    	output.setVisitLineId(resultSet.getInt("VISIT_LINE_ID"));
	    	output.setFirstName(resultSet.getString("FIRST_NAME"));
	    	output.setLastName(resultSet.getString("LAST_NAME"));
	    	output.setDob(resultSet.getString("DOB"));
	    	output.setGender(resultSet.getString("GENDER"));
	    	output.setProvider(resultSet.getString("PROVIDER"));
	    	output.setEncounterStartDate(resultSet.getString("ENCOUNTER_START_DATE"));
	    	output.setEncounterEndDate(resultSet.getString("ENCOUNTER_END_DATE"));
	    	output.setEncounterCode(getListFromString(resultSet.getString("ENCOUNTER_CODE")));
	    	output.setEncounterValueset(getListFromString(resultSet.getString("ENCOUNTER_VALUESET")));
	    	output.setAllergiesDrug(resultSet.getString("ALLERGIES_DRUG"));
	    	output.setAllergiesDrugCode(resultSet.getString("ALLERGIES_DRUG_CODE"));
	    	output.setAllergiesReaction(resultSet.getString("ALLERGIES_REACTION"));
	    	output.setAllergiesStartDate(resultSet.getString("ALLERGIES_START_DATE"));
	    	output.setAllergiesEndDate(resultSet.getString("ALLERGIES_END_DATE"));
	    	output.setAllergiesNotes(resultSet.getString("ALLERGIES_NOTES"));
	    	output.setAllergiesOthers(resultSet.getString("ALLERGIES_OTHERS"));
	    	output.setAllergiesOthersCode(resultSet.getString("ALLERGIES_OTHERS_CODE"));
	    	output.setAllergiesOthersReaction(resultSet.getString("ALLERGIES_OTHERS_REACTION"));
	    	output.setAllergiesOthersStartDate(resultSet.getString("ALLERGIES_OTHERS_START_DATE"));
	    	output.setAllergiesOthersEndDate(resultSet.getString("ALLERGIES_OTHERS_END_DATE"));
	    	output.setAllergies_others_notes(resultSet.getString("ALLERGIES_OTHERS_NOTES"));
	    	output.setMedicationsNdcDrugCodeName(resultSet.getString("MEDICATIONS_NDC_DRUG_CODE_NAME"));
	    	output.setMedicationsDrugCode(resultSet.getString("MEDICATIONS_DRUG_CODE"));
	    	output.setMedicationsDaysSupplied(resultSet.getString("MEDICATIONS_DAYS_SUPPLIED"));
	    	output.setMedicationsQuantity(resultSet.getString("MEDICATIONS_QUANTITY"));
	    	output.setMedicationsNotes(resultSet.getString("MEDICATIONS_NOTES"));
	    	output.setMedicationsPrescriptionDate(resultSet.getString("MEDICATIONS_PRESCRIPTION_DATE"));
	    	output.setMedicationsStartDate(resultSet.getString("MEDICATIONS_START_DATE"));
	    	output.setMedicationsDispensedDate(resultSet.getString("MEDICATIONS_DISPENSED_DATE"));
	    	output.setMedicationsEndDate(resultSet.getString("MEDICATIONS_END_DATE"));
	    	output.setMedicationsProviderNpi(resultSet.getString("MEDICATIONS_PROVIDER_NPI"));
	    	output.setMedicationsPharmacyNpi(resultSet.getString("MEDICATIONS_PHARMACY_NPI"));
	    	output.setImmunizationsName(resultSet.getString("IMMUNIZATIONS_NAME"));
	    	output.setImmunizationsCode(resultSet.getString("IMMUNIZATIONS_CODE"));
	    	output.setImmunizationsStartDate(resultSet.getString("IMMUNIZATIONS_START_DATE"));
	    	output.setImmunizationsEndDate(resultSet.getString("IMMUNIZATIONS_END_DATE"));
	    	output.setImmunizationsNotes(resultSet.getString("IMMUNIZATIONS_NOTES"));
	    	output.setDisease(resultSet.getString("DISEASE"));
	    	output.setDiseaseCode(resultSet.getString("DISEASE_CODE"));
	    	output.setDiseaseDiagnosisDate(resultSet.getString("DISEASE_DIAGNOSIS_DATE"));
	    	output.setDiseaseEndDate(resultSet.getString("DISEASE_END_DATE"));
	    	output.setDiseaseNotes(resultSet.getString("DISEASE_NOTES"));
	    	output.setSdohCategory(resultSet.getString("SDOH_CATEGORY"));
	    	output.setSdohCategoryCode(resultSet.getString("SDOH_CATEGORY_CODE"));
	    	output.setSdohProblem(resultSet.getString("SDOH_PROBLEM"));
	    	output.setSdohProblemCode(resultSet.getString("SDOH_PROBLEM_CODE"));
	    	output.setSdohNotes(resultSet.getString("SDOH_NOTES"));
	    	output.setSubstanceUse(resultSet.getString("SUBSTANCE_USE"));
	    	output.setSubstanceUseCode(resultSet.getString("SUBSTANCE_USE_CODE"));
	    	output.setSubstanceFrequency(resultSet.getString("SUBSTANCE_FREQUENCY"));
	    	output.setSubstanceNotes(resultSet.getString("SUBSTANCE_NOTES"));
	    	output.setFamilyHistoryRelation(resultSet.getString("FAMILY_HISTORY_RELATION"));
	    	output.setFamilyHistoryStatus(resultSet.getString("FAMILY_HISTORY_STATUS"));
	    	output.setFamilyHistoryDisease(resultSet.getString("FAMILY_HISTORY_DISEASE"));
	    	output.setFamilyHistoryDiseaseCode(resultSet.getString("FAMILY_HISTORY_DISEASE_CODE"));
	    	output.setFamilyHistoryNotes(resultSet.getString("FAMILY_HISTORY_NOTES"));		    	
	    	output.setChartType(resultSet.getString("CHART_TYPE"));
	    	output.setProvId(resultSet.getString("PROV_ID"));
	    	
		    setOutput.add(output);
		}	
		return setOutput;
	}

}
