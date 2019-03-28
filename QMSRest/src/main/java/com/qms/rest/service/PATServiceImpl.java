package com.qms.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.CloseGap;
import com.qms.rest.model.DimMemeber;
import com.qms.rest.model.NameValue;
import com.qms.rest.model.Pat;
import com.qms.rest.model.PatActionCareGap;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.SearchAssociatedPatient;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;
import com.qms.rest.util.QMSDateUtil;

@Service("patService")
public class PATServiceImpl implements PATService {
	
	@Autowired
	private QMSConnection qmsConnection;
	
	@Autowired 
	private HttpSession httpSession;
	
	@Autowired
	CloseGapsService closeGapsService;	

	@Override
	public Set<NameValue> getPopulationList() {
		Set<NameValue> categorySet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "SELECT DISTINCT DPP.LOB_PRODUCT,DPP.PLAN_NAME FROM FACT_HEDIS_GAPS_IN_CARE FHGIC "+ 
					"INNER JOIN DIM_PRODUCT_PLAN DPP ON FHGIC.PRODUCT_PLAN_SK = DPP.PRODUCT_PLAN_SK "+ 
					"INNER JOIN REF_LOB RL ON DPP.LOB_ID = RL.LOB_ID";			
			resultSet = statement.executeQuery(query);
			NameValue nameValue = null; 
			while (resultSet.next()) {
				nameValue = new NameValue();
				nameValue.setName(resultSet.getString("LOB_PRODUCT"));
				nameValue.setValue(resultSet.getString("PLAN_NAME"));				
				categorySet.add(nameValue);				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return categorySet;
	}

	@Override
	public Set<NameValue> getCareGapList() {
		Set<NameValue> categorySet = new HashSet<>();		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "SELECT DISTINCT QQM.QUALITY_MEASURE_ID,QQM.MEASURE_NAME FROM QMS_GIC_LIFECYCLE FHGIC "+
					       "INNER JOIN QMS_QUALITY_MEASURE QQM ON QQM.QUALITY_MEASURE_ID = FHGIC.QUALITY_MEASURE_ID";
			resultSet = statement.executeQuery(query);
			NameValue nameValue = null; 
			while (resultSet.next()) {
				nameValue = new NameValue();
				nameValue.setName(resultSet.getString("MEASURE_NAME"));
				nameValue.setValue(resultSet.getString("QUALITY_MEASURE_ID"));
				categorySet.add(nameValue);				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return categorySet;
	}

	@Override
	public Set<SearchAssociatedPatient> searchAssociatedPatientList(String measureId, String mrnIdOrName) {
		Set<SearchAssociatedPatient> categorySet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "SELECT DISTINCT DP.PAT_ID, (DP.PAT_FIRST_NAME||' '||DP.PAT_MIDDLE_NAME||' '||DP.PAT_LAST_NAME) AS NAME, "+
						"DP.PAT_MRN FROM QMS_GIC_LIFECYCLE FHGIC "+
						"INNER JOIN DIM_MEMBER DM ON DM.MEMBER_ID = FHGIC.MEMBER_ID "+
						"INNER JOIN FACT_PAT_MEM FPM ON FPM.MEMBER_ID = DM.MEMBER_ID "+
						"INNER JOIN DIM_PATIENT DP ON DP.PAT_ID = FPM.PAT_ID "+
						"INNER JOIN QMS_QUALITY_MEASURE QQM ON QQM.QUALITY_MEASURE_ID = FHGIC.QUALITY_MEASURE_ID "+
						"where (QQM.QUALITY_MEASURE_ID = '"+measureId+"') and "+
						"(DP.PAT_MRN LIKE '"+mrnIdOrName+ "%' or "+
						"DP.PAT_FIRST_NAME LIKE '"+mrnIdOrName+"%' or "+
						"DP.PAT_MIDDLE_NAME LIKE '"+mrnIdOrName+"%' or "+
						"DP.PAT_MIDDLE_NAME LIKE '"+mrnIdOrName+"%') order by DP.PAT_ID asc";
			resultSet = statement.executeQuery(query);
			SearchAssociatedPatient searchAssociatedPatient = null;
			while (resultSet.next()) {
				searchAssociatedPatient = new SearchAssociatedPatient();
				searchAssociatedPatient.setName(resultSet.getString("NAME"));
				searchAssociatedPatient.setPatId(resultSet.getString("PAT_ID"));
				searchAssociatedPatient.setPatMrn(resultSet.getString("PAT_MRN"));
				categorySet.add(searchAssociatedPatient);				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return categorySet;
	}

	@Override
	public PatActionCareGap actionOnCareGapList(String measureId) {
		PatActionCareGap patActionCareGap = new PatActionCareGap();
		Set<String> valueSet = new TreeSet<>();
		Set<String> codeType = new TreeSet<>();
		Set<String> codes = new TreeSet<>();	
		patActionCareGap.setCodes(codes);
		patActionCareGap.setCodeType(codeType);
		patActionCareGap.setValueSet(valueSet);
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "SELECT DISTINCT QQM.QUALITY_MEASURE_ID, HEDIS.VALUESET, HEDIS.CODESYSTEM, HEDIS.CODE "+
						   "FROM QMS_GIC_LIFECYCLE FHGIC "+
						   "INNER JOIN QMS_QUALITY_MEASURE QQM ON QQM.QUALITY_MEASURE_ID = FHGIC.QUALITY_MEASURE_ID "+
						   "INNER JOIN REF_HEDIS2019 HEDIS ON HEDIS.MEASURENAME = QQM.MEASURE_NAME "+
						   "where QQM.QUALITY_MEASURE_ID='"+measureId+"'";
			System.out.println(" actionOnCareGapList - measureId "+measureId);
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				valueSet.add(resultSet.getString("VALUESET"));
				codeType.add(resultSet.getString("CODESYSTEM"));
				codes.add(resultSet.getString("CODE"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return patActionCareGap;
	}	
	

	@Override
	public RestResult insertPatCreator(Pat pat) {
		
		String sqlStatementInsert = 
				"insert into QMS_MIT(MIT_ID,MEASURE_ID,PATIENT_ID,LOB_ID,MRN,APPOINTMENT_DATE,PROVIDER_ID,"
				+ "GENDER,DOB,MEMBER_STATUS,VALUE_SET,CODE_TYPE,CODES,REASON,CURR_FLAG,REC_CREATE_DATE,"
				+ "REC_UPDATE_DATE,LATEST_FLAG,ACTIVE_FLAG,INGESTION_DATE,SOURCE_NAME,USER_NAME,COMPLIANT_FLAG) " 		
				+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		PreparedStatement statement = null;
		Connection connection = null;
		ResultSet resultSet = null;
		Statement getStatement = null;
		
		try {	
			connection = qmsConnection.getOracleConnection();
				
			int mitId = 0;
			getStatement = connection.createStatement();
			resultSet = getStatement.executeQuery("select max(MIT_ID)from QMS_MIT");
			while(resultSet.next()) {
				mitId = resultSet.getInt(1);
			}
			mitId = mitId + 1;
		
			int i=0;
			Date date = new Date();				
			Timestamp timestamp = new Timestamp(date.getTime());	
			statement = connection.prepareStatement(sqlStatementInsert);
			
			statement.setInt(++i, mitId);
			statement.setString(++i, pat.getMeasureSk());
			statement.setString(++i, pat.getPatientId());			
			statement.setString(++i, pat.getLobId());
			statement.setString(++i, pat.getMrn());
			statement.setString(++i, pat.getAppointmentDate());
			statement.setString(++i, pat.getProviderId());
			statement.setString(++i, pat.getGender());
			statement.setString(++i, pat.getDob());
			statement.setString(++i, pat.getMemberStatus());
			statement.setString(++i, pat.getValueSet());
			statement.setString(++i, pat.getCodeType());
			statement.setString(++i, pat.getCodes());
			statement.setString(++i, pat.getReason());
			statement.setString(++i, "Y");
			statement.setTimestamp(++i, timestamp);
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i,"Y");
			statement.setString(++i,"A");
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i, "UI");	
			if(userData != null && userData.getName() != null)
				statement.setString(++i, userData.getName());
			else 
				statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
			statement.setString(++i, pat.getCompliantFlag()); //COMPLIANT_FLAG
			statement.executeUpdate();
			
			httpSession.setAttribute(QMSConstants.SESSION_TYPE_ID, mitId+"");
			System.out.println(" Inserting QMS_MIT sucess for ID --> " + mitId);
			return RestResult.getSucessRestResult("MIT creation success. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());		
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, getStatement, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}	
	}	


	@Override
	public DimMemeber findMembergapListByMid(String memberId) {
		DimMemeber dimMemeberList = new DimMemeber();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			
			String membergplistQry = "SELECT DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME) AS NAME, DM.GENDER, DD.DATE_SK,"
			+"(TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')) AS DATE_OF_BIRTH,"
			+"GIC1.QUALITY_MEASURE_ID, (GIC1.INTERVENTIONS) AS INTERVENTIONS, GIC1.STATUS AS STATUS, QM.MEASURE_NAME, MIN(GIC1.GAP_DATE) AS START_DATE, MAX(GIC2.GAP_DATE) AS END_DATE,"
			+"FLOOR(SYSDATE - MAX(CAST(GIC1.GAP_DATE AS DATE))) AS DURATION, GIC1.PRIORITY AS PRIORITY, GIC1.TARGET_DATE " 
			+"FROM DIM_MEMBER DM "
			+"INNER JOIN QMS_GIC_LIFECYCLE GIC1 ON GIC1.MEMBER_ID = DM.MEMBER_ID " 
			+"LEFT OUTER JOIN QMS_GIC_LIFECYCLE GIC2 ON GIC1.QUALITY_MEASURE_ID = GIC2.QUALITY_MEASURE_ID AND GIC1.MEMBER_ID = GIC2.MEMBER_ID " 
			+"INNER JOIN QMS_QUALITY_MEASURE QM ON GIC1.QUALITY_MEASURE_ID=QM.QUALITY_MEASURE_ID " 
			+"INNER JOIN DIM_DATE DD ON DD.DATE_SK = DM.DATE_OF_BIRTH_SK " 
			+"WHERE DM.MEMBER_ID='"+memberId+"' " 
			+"GROUP BY DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME), DM.GENDER, DD.DATE_SK, GIC1.PRIORITY,GIC1.INTERVENTIONS, GIC1.STATUS,"
			+"(TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')), GIC1.QUALITY_MEASURE_ID, QM.MEASURE_NAME, GIC1.TARGET_DATE " 
			+"order by max(GIC1.GAP_DATE) desc";
			
			resultSet = statement.executeQuery(membergplistQry);
			if (resultSet.next()) {
				dimMemeberList.setMemberId(resultSet.getString("MEMBER_ID"));
				dimMemeberList.setName(resultSet.getString("name"));
				dimMemeberList.setGender(resultSet.getString("gender") != null ? resultSet.getString("gender").trim() : null);
				dimMemeberList.setDateOfBirthSk(QMSDateUtil.getSQLDateFormat(resultSet.getDate("DATE_OF_BIRTH")));
			}

			resultSet.close();
			membergplistQry = "SELECT DDA.CALENDAR_DATE AS \"Next_Appointment_Date\", "
					+ "(DP.FIRST_NAME||' '||DP.LAST_NAME) AS \"Physician_Name\", "
					+ "DD.DEPARTMENT_NAME, FA.NOSHOW_LIKELIHOOD, FA.NOSHOW, DPA.PAT_ID, DPA.PAT_MRN "+ 
					"FROM FACT_APPOINTMENT FA "+ 
					"INNER JOIN DIM_DEPARTMENT DD ON DD.DEPARTMENT_SK = FA.DEPARTMENT_SK "+ 
					"INNER JOIN DIM_PROVIDER DP ON DP.PROVIDER_SK = FA.PROVIDER_SK "+
					"INNER JOIN DIM_DATE DDA ON DDA.DATE_SK=FA.APPOINTMENT_DATE_SK "+
					"INNER JOIN DIM_PATIENT DPA ON DPA.PATIENT_SK=FA.PATIENT_SK "+
					"WHERE DDA.CALENDAR_DATE>SYSDATE AND DPA.PAT_ID='"+memberId+"'";			
			resultSet = statement.executeQuery(membergplistQry);
			while (resultSet.next()) {
				//dimMemeberList.setNextAppointmentDate(resultSet.getString("Next_Appointment_Date"));
				dimMemeberList.setNextAppointmentDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("Next_Appointment_Date")));
				dimMemeberList.setPcpName(resultSet.getString("Physician_Name"));
				dimMemeberList.setMrn(resultSet.getString("PAT_MRN"));
			}
			
	
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}

		return dimMemeberList;
	}	

	@Override
	public List<Pat> getPatById(String patientId, String measureId) {
		List<Pat> mitList = new LinkedList<>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		Pat pat = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			System.out.println(patientId + " patientId. getPatById. measureId " + measureId);
			String membergplistQry = "SELECT MIT_ID,MEASURE_ID,PATIENT_ID,COMPLIANT_FLAG,LOB_ID,MRN,APPOINTMENT_DATE,PROVIDER_ID,GENDER,DOB,"
				+ "MEMBER_STATUS,VALUE_SET,CODE_TYPE,CODES,REASON "
				+ "from QMS_MIT where PATIENT_ID="+patientId+" and MEASURE_ID='"+measureId+"' order by REC_CREATE_DATE desc";
			resultSet = statement.executeQuery(membergplistQry);
			List<Integer> mitIds = new ArrayList<>();
			while(resultSet.next()) {
				pat = new Pat();
				pat.setMitId(resultSet.getString("MIT_ID"));
				mitIds.add(Integer.parseInt(pat.getMitId()));
				pat.setMeasureSk(resultSet.getString("MEASURE_ID"));
				pat.setPatientId(resultSet.getString("PATIENT_ID"));
				pat.setCompliantFlag(resultSet.getString("COMPLIANT_FLAG"));
				pat.setLobId(resultSet.getString("LOB_ID"));
				pat.setMrn(resultSet.getString("MRN"));
				pat.setAppointmentDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("APPOINTMENT_DATE")));
				pat.setProviderId(resultSet.getString("PROVIDER_ID"));
				pat.setGender(resultSet.getString("GENDER"));
				pat.setDob(QMSDateUtil.getSQLDateFormat(resultSet.getDate("DOB")));
				pat.setMemberStatus(resultSet.getString("MEMBER_STATUS"));
				pat.setValueSet(resultSet.getString("VALUE_SET"));
				pat.setCodeType(resultSet.getString("CODE_TYPE"));
				pat.setCodes(resultSet.getString("CODES"));
				pat.setReason(resultSet.getString("REASON"));
				mitList.add(pat);
			}
			if(mitIds.size() > 0) {
				HashMap<Integer, List<String>> upLoadData = closeGapsService.getUploadFileByTypeId(statement, mitIds, 0, "mit");
				if(upLoadData != null) {
					List<String> fileNames = null;
					for (Pat pat1 : mitList) {
						fileNames = upLoadData.get(Integer.parseInt(pat1.getMitId()));
						if(fileNames != null && !fileNames.isEmpty()) {
							pat1.getUploadFilesList().addAll(fileNames);
						}
					}
				}				
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		
		return mitList;		
	}
	

	@Override
	public Set<String> actionOnCareGapCodeTypeList(String measureId ,String valueSet) {
		Set<String> codeType1 = new TreeSet<>();
	
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "SELECT DISTINCT HEDIS.CODESYSTEM FROM QMS_GIC_LIFECYCLE FHGIC "+
						   "INNER JOIN QMS_QUALITY_MEASURE QQM ON QQM.QUALITY_MEASURE_ID = FHGIC.QUALITY_MEASURE_ID "+
						   "INNER JOIN REF_HEDIS2019 HEDIS ON HEDIS.MEASURENAME = QQM.MEASURE_NAME "+
						   "where QQM.QUALITY_MEASURE_ID='"+measureId+"' and HEDIS.VALUESET='"+valueSet+"'";
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				codeType1.add(resultSet.getString("CODESYSTEM"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return codeType1;
	}	
	
	@Override
	public Set<String> actionOnCareGapCodesList(String measureId ,String valueSet,String codeTypeem) {
		Set<String> codes1 = new TreeSet<>();	
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "SELECT DISTINCT HEDIS.CODE FROM QMS_GIC_LIFECYCLE FHGIC "+
						   "INNER JOIN QMS_QUALITY_MEASURE QQM ON QQM.QUALITY_MEASURE_ID = FHGIC.QUALITY_MEASURE_ID "+
						   "INNER JOIN REF_HEDIS2019 HEDIS ON HEDIS.MEASURENAME = QQM.MEASURE_NAME "+
						   "where QQM.QUALITY_MEASURE_ID='"+measureId+"' and HEDIS.VALUESET='"+valueSet+"' and HEDIS.CODESYSTEM='"+codeTypeem+"'";
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				codes1.add(resultSet.getString("CODE"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return codes1;
	}	
	
}
