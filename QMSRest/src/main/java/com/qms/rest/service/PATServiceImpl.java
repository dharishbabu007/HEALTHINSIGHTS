package com.qms.rest.service;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.qms.rest.model.DimMemeber;
import com.qms.rest.model.DimMemeberList;
import com.qms.rest.model.FactHedisGapsInCare;
import com.qms.rest.model.NameValue;
import com.qms.rest.model.Pat;
import com.qms.rest.model.PatActionCareGap;
import com.qms.rest.model.PatFileUpload;
import com.qms.rest.model.QmsGicLifecycle;
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

	@Override
	public Set<String> getPopulationList() {
		Set<String> categorySet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "SELECT DISTINCT DPP.LOB_PRODUCT FROM FACT_HEDIS_GAPS_IN_CARE FHGIC "+ 
					"INNER JOIN DIM_PRODUCT_PLAN DPP ON FHGIC.PRODUCT_PLAN_SK = DPP.PRODUCT_PLAN_SK "+ 
					"INNER JOIN REF_LOB RL ON DPP.LOB_ID = RL.LOB_ID";			
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				categorySet.add(resultSet.getString("LOB_PRODUCT"));				
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
			String query = "SELECT DISTINCT QQM.QUALITY_MEASURE_SK,QQM.MEASURE_TITLE FROM FACT_HEDIS_GAPS_IN_CARE FHGIC "+
					       "INNER JOIN DIM_QUALITY_MEASURE QQM ON QQM.QUALITY_MEASURE_SK = FHGIC.QUALITY_MEASURE_SK";
			resultSet = statement.executeQuery(query);
			NameValue nameValue = null; 
			while (resultSet.next()) {
				nameValue = new NameValue();
				nameValue.setName(resultSet.getString("MEASURE_TITLE"));
				nameValue.setValue(resultSet.getString("QUALITY_MEASURE_SK"));
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
	public Set<SearchAssociatedPatient> searchAssociatedPatientList(String measureSK, String mrnIdOrName) {
		Set<SearchAssociatedPatient> categorySet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String query = "SELECT DISTINCT DP.PAT_ID, (DP.PAT_FIRST_NAME||' '||DP.PAT_MIDDLE_NAME||' '||DP.PAT_LAST_NAME) AS NAME, "+
						"DP.PAT_MRN FROM FACT_HEDIS_GAPS_IN_CARE FHGIC "+
						"INNER JOIN DIM_MEMBER DM ON DM.MEMBER_SK = FHGIC.MEMBER_SK "+
						"INNER JOIN FACT_PAT_MEM FPM ON FPM.MEMBER_ID = DM.MEMBER_ID "+
						"INNER JOIN DIM_PATIENT DP ON DP.PAT_ID = FPM.PAT_ID "+
						"INNER JOIN DIM_QUALITY_MEASURE QQM ON QQM.QUALITY_MEASURE_SK = FHGIC.QUALITY_MEASURE_SK "+
						"where (QQM.QUALITY_MEASURE_SK = '"+measureSK+"') and "+
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
	public PatActionCareGap actionOnCareGapList(String measureSK) {
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
			String query = "SELECT DISTINCT QQM.QUALITY_MEASURE_SK, HEDIS.VALUESET, HEDIS.CODESYSTEM, HEDIS.CODE "+
						   "FROM FACT_HEDIS_GAPS_IN_CARE FHGIC "+
						   "INNER JOIN DIM_QUALITY_MEASURE QQM ON QQM.QUALITY_MEASURE_SK = FHGIC.QUALITY_MEASURE_SK "+
						   "INNER JOIN REF_HEDIS2019 HEDIS ON HEDIS.MEASURENAME = QQM.MEASURE_TITLE "+
						   "where QQM.QUALITY_MEASURE_SK='"+measureSK+"'";
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
				"insert into QMS_MIT(MIT_ID,MEASURE_SK,PATIENT_ID,LOB_ID,MRN,APPOINTMENT_DATE,PROVIDER_ID,"
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
			statement.setString(++i, "Y"); //COMPLIANT_FLAG
			statement.executeUpdate();
			
			httpSession.setAttribute(QMSConstants.SESSION_TYPE_ID, mitId+"");			
			return RestResult.getSucessRestResult("MIT creation sucess. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());		
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, getStatement, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}	
	}	


	public RestResult importFile(MultipartFile uploadFile) {
		try {
			String patId = (String)httpSession.getAttribute(QMSConstants.SESSION_PAT_ID);
			if(patId != null) {
				String path = "D:/import_export/PAT/";
				PatFileUpload fileUpload = new PatFileUpload();
				fileUpload.setFileName(uploadFile.getOriginalFilename());
				fileUpload.setFilePath(path);
				fileUpload.setPatId(patId);
				saveFileUpload(fileUpload);
				System.out.println(patId+" PAT DB success");
				
				String parentDir = path+patId;
				File parentDirFile = new File(parentDir);		
				if(!parentDirFile.exists()) {
					parentDirFile.mkdir();
					System.out.println(patId+" upload dir created..");
				}
				
				FileOutputStream out = new FileOutputStream(parentDir+"/"+uploadFile.getOriginalFilename());
				out.write(uploadFile.getBytes());
				out.close();	
				System.out.println(" file created success in windows ..");
				httpSession.removeAttribute(QMSConstants.SESSION_PAT_ID);
				return RestResult.getSucessRestResult(" File Upload Success.. ");			
			} else {
				return RestResult.getFailRestResult(" Update close gap status failed. ");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		} 
	}

	@Override
	public RestResult saveFileUpload(PatFileUpload fileUpload) {		
		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = null;
		Statement statementObj = null;
		ResultSet resultSet = null;
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		try {
			connection = qmsConnection.getOracleConnection();	
			statementObj = connection.createStatement();			
			
			resultSet = statementObj.executeQuery("select max(FILE_ID) from QMS_PAT_FILE_UPLOAD");
			int fileId = 0;
			while (resultSet.next()) {
				fileId = resultSet.getInt(1)+1;
			}
			if(fileId == 0) fileId = 1;
			resultSet.close();			
			System.out.println(" Adding the file upload with file id --> " + fileId);
			
			String sqlStatementInsert = "insert into QMS_PAT_FILE_UPLOAD "
					                    + "(FILE_ID,PAT_ID,FILE_PATH,FILE_NAME,CREATION_DATE,CURR_FLAG,"
										+ "REC_CREATE_DATE,REC_UPDATE_DATE,LATEST_FLAG,ACTIVE_FLAG,"
										+ "INGESTION_DATE,SOURCE_NAME,USER_NAME)"						
										+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
			statement = connection.prepareStatement(sqlStatementInsert);
			int i=0;
			statement.setInt(++i, fileId);
		//	statement.setInt(++i, fileUpload.patId());
			statement.setString(++i, fileUpload.getFilePath());
			statement.setString(++i, fileUpload.getFileName());
			Date date = new Date();				
			Timestamp timestamp = new Timestamp(date.getTime());				
			statement.setTimestamp(++i, timestamp);
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
			
			statement.executeUpdate();
			restResult = RestResult.getSucessRestResult("PAT UPLOAD added successfully.");
		} catch (Exception e) {
			restResult = RestResult.getFailRestResult(e.getMessage());
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statementObj, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}	
		return restResult;		

	}
	
	
	@Override
	public DimMemeber findMembergapListByMid(String mid) {
		DimMemeber dimMemeberList = new DimMemeber();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			
			String membergplistQry = "SELECT DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME) AS NAME, DM.GENDER, DD.DATE_SK,"
			+"(TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')) AS DATE_OF_BIRTH,"
			+"GIC1.QUALITY_MEASURE_ID, (GIC1.INTERVENTIONS) AS INTERVENTIONS, GIC1.STATUS AS STATUS, QM.MEASURE_TITLE, MIN(GIC1.GAP_DATE) AS START_DATE, MAX(GIC2.GAP_DATE) AS END_DATE,"
			+"FLOOR(SYSDATE - MAX(CAST(GIC1.GAP_DATE AS DATE))) AS DURATION, GIC1.PRIORITY AS PRIORITY, GIC1.TARGET_DATE " 
			+"FROM DIM_MEMBER DM "
			+"INNER JOIN QMS_GIC_LIFECYCLE GIC1 ON GIC1.MEMBER_ID = DM.MEMBER_ID " 
			+"LEFT OUTER JOIN QMS_GIC_LIFECYCLE GIC2 ON GIC1.QUALITY_MEASURE_ID = GIC2.QUALITY_MEASURE_ID AND GIC1.MEMBER_ID = GIC2.MEMBER_ID " 
			+"INNER JOIN DIM_QUALITY_MEASURE QM ON GIC1.QUALITY_MEASURE_ID=QM.QUALITY_MEASURE_ID " 
			+"INNER JOIN DIM_DATE DD ON DD.DATE_SK = DM.DATE_OF_BIRTH_SK " 
			+"WHERE DM.MEMBER_ID='"+mid+"' " 
			+"GROUP BY DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME), DM.GENDER, DD.DATE_SK, GIC1.PRIORITY,GIC1.INTERVENTIONS, GIC1.STATUS,"
			+"(TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')), GIC1.QUALITY_MEASURE_ID, QM.MEASURE_TITLE, GIC1.TARGET_DATE " 
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
					+ "DD.DEPARTMENT_NAME, FA.NOSHOW_LIKELIHOOD, FA.NOSHOW, DPA.PAT_ID "+ 
					"FROM FACT_APPOINTMENT FA "+ 
					"INNER JOIN DIM_DEPARTMENT DD ON DD.DEPARTMENT_SK = FA.DEPARTMENT_SK "+ 
					"INNER JOIN DIM_PROVIDER DP ON DP.PROVIDER_SK = FA.PROVIDER_SK "+
					"INNER JOIN DIM_DATE DDA ON DDA.DATE_SK=FA.APPOINTMENT_DATE_SK "+
					"INNER JOIN DIM_PATIENT DPA ON DPA.PATIENT_SK=FA.PATIENT_SK "+
					"WHERE DDA.CALENDAR_DATE>SYSDATE AND DPA.PAT_ID='"+mid+"'";			
			resultSet = statement.executeQuery(membergplistQry);
			while (resultSet.next()) {
				//dimMemeberList.setNextAppointmentDate(resultSet.getString("Next_Appointment_Date"));
				dimMemeberList.setNextAppointmentDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("Next_Appointment_Date")));
				dimMemeberList.setPcpName(resultSet.getString("Physician_Name"));
			}
	
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}

		return dimMemeberList;
	}	

	@Override
	public List<Pat> getPatById(String patientId, String measureSK) {
		List<Pat> mitList = new LinkedList<>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		Pat pat = null;
		try {
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			String membergplistQry = "SELECT MIT_ID,MEASURE_SK,PATIENT_ID,COMPLIANT_FLAG,LOB_ID,MRN,APPOINTMENT_DATE,PROVIDER_ID,GENDER,DOB,"
				+ "MEMBER_STATUS,VALUE_SET,CODE_TYPE,CODES,REASON "
				+ "from QMS_MIT where PATIENT_ID='"+patientId+"' and MEASURE_SK='"+measureSK+"' order by REC_CREATE_DATE desc";
			resultSet = statement.executeQuery(membergplistQry);
			while(resultSet.next()) {
				pat = new Pat();
				pat.setMitId(resultSet.getString("MIT_ID"));
				pat.setMeasureSk(resultSet.getString("MEASURE_SK"));
				pat.setPatientId(resultSet.getString("PATIENT_ID"));
				pat.setCompliantFlag(resultSet.getString("COMPLIANT_FLAG"));
				pat.setLobId(resultSet.getString("LOB_ID"));
				pat.setLobId(resultSet.getString("MRN"));
				pat.setAppointmentDate(resultSet.getString("APPOINTMENT_DATE"));
				pat.setProviderId(resultSet.getString("PROVIDER_ID"));
				pat.setGender(resultSet.getString("GENDER"));
				pat.setDob(resultSet.getString("DOB"));
				pat.setMemberStatus(resultSet.getString("MEMBER_STATUS"));
				pat.setValueSet(resultSet.getString("VALUE_SET"));
				pat.setCodeType(resultSet.getString("CODE_TYPE"));
				pat.setCodes(resultSet.getString("CODES"));
				pat.setReason(resultSet.getString("REASON"));
				mitList.add(pat);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		
		return mitList;		
	}
	
	
	
}
