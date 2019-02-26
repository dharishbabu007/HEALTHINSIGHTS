package com.qms.rest.service;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.qms.rest.model.CloseGap;
import com.qms.rest.model.CloseGaps;
import com.qms.rest.model.GicLifeCycleFileUpload;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;
import com.qms.rest.util.QMSDateUtil;

@Service("closeGapsService")
public class CloseGapsServiceImpl2 implements CloseGapsService {
	
	@Autowired
	private QMSConnection qmsConnection;	
	
	@Autowired 
	private HttpSession httpSession;	
	
	@Autowired
	private UserRoleService userRoleService;

	@Override
	public CloseGaps getCloseGaps(String memberId, String measureId) {
		
		CloseGaps closeGaps = new CloseGaps();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select dm.gender, dm.member_id, (dm.FIRST_NAME||' '||dm.MIDDLE_NAME||' '||dm.LAST_NAME) AS NAME, "
					+ "dd.CALENDAR_DATE from dim_member dm, dim_date dd "
					+ "where dm.date_of_birth_sk=dd.date_sk and member_id='"+memberId+"'");
			if (resultSet.next()) {
				closeGaps.setGender(resultSet.getString("gender"));
				closeGaps.setDateOfBirth(resultSet.getString("CALENDAR_DATE"));
				closeGaps.setMemberId(resultSet.getString("member_id"));
				closeGaps.setName(resultSet.getString("NAME"));
			}
			
			resultSet.close();
			String membergplistQry = "SELECT MIN(GIC.GAP_DATE) AS OPENDATE,DQM.MEASURE_TITLE,GIC.TARGET_DATE,GIC.STATUS,GIC.GAP_DATE,"+
					"(DP.FIRST_NAME||' '||DP.LAST_NAME) AS PCP FROM DIM_QUALITY_MEASURE DQM "+
					"INNER JOIN QMS_GIC_LIFECYCLE GIC ON GIC.QUALITY_MEASURE_ID = DQM.QUALITY_MEASURE_ID "+
					"INNER JOIN DIM_MEMBER ENC ON ENC.MEMBER_ID = GIC.MEMBER_ID "+
					"INNER JOIN FACT_MEM_ATTRIBUTION FMA ON FMA.MEMBER_SK = ENC.MEMBER_SK "+ 
					"INNER JOIN DIM_PROVIDER DP ON DP.PROVIDER_SK = FMA.PROVIDER_SK "+
					"WHERE ENC.MEMBER_ID='"+memberId+"' AND GIC.QUALITY_MEASURE_ID='"+measureId+"' "+
					"GROUP BY DQM.MEASURE_TITLE,GIC.TARGET_DATE,GIC.STATUS,GIC.GAP_DATE,(DP.FIRST_NAME||' '||DP.LAST_NAME) "+
					"ORDER BY GIC.GAP_DATE DESC";
			resultSet = statement.executeQuery(membergplistQry);
			//measure details			
			if (resultSet.next()) {
				closeGaps.setCareGap(resultSet.getString("measure_title")); 
				closeGaps.setOpenDate(resultSet.getString("OPENDATE"));
				closeGaps.setTargetDate(resultSet.getString("TARGET_DATE"));
				closeGaps.setAssignedTo(resultSet.getString("PCP")); 
				closeGaps.setStatus(resultSet.getString("STATUS")); 
				closeGaps.setLastActionDate(resultSet.getString("GAP_DATE"));
			}
			
			resultSet.close();
			membergplistQry = "SELECT DDA.CALENDAR_DATE AS \"Next_Appointment_Date\", (DP.FIRST_NAME||' '||DP.LAST_NAME) AS \"Physician_Name\", DD.DEPARTMENT_NAME, FA.NOSHOW_LIKELIHOOD, FA.NOSHOW, DPA.PAT_ID "+ 
					"FROM FACT_APPOINTMENT FA "+ 
					"INNER JOIN DIM_DEPARTMENT DD ON DD.DEPARTMENT_SK = FA.DEPARTMENT_SK "+ 
					"INNER JOIN DIM_PROVIDER DP ON DP.PROVIDER_SK = FA.PROVIDER_SK "+
					"INNER JOIN DIM_DATE DDA ON DDA.DATE_SK=FA.APPOINTMENT_DATE_SK "+
					"INNER JOIN DIM_PATIENT DPA ON DPA.PATIENT_SK=FA.PATIENT_SK "+
					"WHERE DDA.CALENDAR_DATE>SYSDATE AND DPA.PAT_ID='"+memberId+"'";			
			resultSet = statement.executeQuery(membergplistQry);
			while (resultSet.next()) {
				closeGaps.setNextAppointmentDate(resultSet.getString("Next_Appointment_Date"));
			}						
			
			resultSet.close();	
			if(measureId == null || measureId.equalsIgnoreCase("0") || measureId.equalsIgnoreCase("all")) {
				resultSet = statement.executeQuery("select qgl.*, dqm.measure_title from qms_gic_lifecycle qgl, "
						+ "dim_quality_measure dqm where dqm.quality_measure_id = qgl.quality_measure_id and "
						+ "member_id='"+memberId+"' order by gap_date desc");
			} else {
				resultSet = statement.executeQuery("select qgl.*, dqm.measure_title from qms_gic_lifecycle qgl, "
						+ "dim_quality_measure dqm where dqm.quality_measure_id = qgl.quality_measure_id and "
						+ "qgl.quality_measure_id = '"+measureId+"' and  "
						+ "member_id='"+memberId+"' order by gap_date desc");				
			}
			CloseGap closeGap = null;
			Set<CloseGap> closeGapSet = new LinkedHashSet<>();
			List<Integer> lifeCycleIds = new ArrayList<>();
			while (resultSet.next()) {
				closeGap = new CloseGap();
				closeGap.setLifeCycleId(resultSet.getInt("GIC_LIFECYCLE_ID"));
				lifeCycleIds.add(closeGap.getLifeCycleId());
				closeGap.setMeasureTitle(resultSet.getString("measure_title"));
				closeGap.setQualityMeasureId(resultSet.getString("quality_measure_id"));
				closeGap.setPayerComments(resultSet.getString("payor_comments"));
				closeGap.setProviderComments(resultSet.getString("provider_comments"));
				closeGap.setGapDate(resultSet.getString("gap_date"));
				closeGap.setIntervention(resultSet.getString("interventions"));
				closeGap.setPriority(resultSet.getString("priority"));
				String dateStr = QMSDateUtil.getSQLDateFormat(resultSet.getDate("TARGET_DATE"));
				closeGap.setTargetDate(dateStr);
				//closeGap.setStatus(resultSet.getString("status"));
				closeGapSet.add(closeGap);				
			}
			closeGaps.setCareGaps(closeGapSet);
			if(lifeCycleIds.size() > 0) {
//				resultSet.close();
//				String glcIds = lifeCycleIds.toString().substring(1, (lifeCycleIds.toString().length()-1));
//				resultSet = statement.executeQuery("select GIC_LIFECYCLE_ID,FILE_PATH,FILE_NAME from "
//						+ "QMS_GIC_FILE_UPLOAD where GIC_LIFECYCLE_ID in("+glcIds+")");
//				int lifeCycleId = 0;
//				while (resultSet.next()) {
//					lifeCycleId = resultSet.getInt("GIC_LIFECYCLE_ID");
//					for (CloseGap closeGap1 : closeGapSet) {
//						if(closeGap1.getLifeCycleId() == lifeCycleId) {
//							closeGap1.getUploadList().add(resultSet.getString("FILE_PATH")+
//									resultSet.getString("GIC_LIFECYCLE_ID")+"/"+
//									resultSet.getString("FILE_NAME"));
//							break;
//						}
//					}
//				}
				
				HashMap<Integer, List<String>> upLoadData = getUploadFileByTypeId(statement, lifeCycleIds, 0, "close_gap");
				if(upLoadData != null) {
					List<String> fileNames = null;
					for (CloseGap closeGap1 : closeGapSet) {
						fileNames = upLoadData.get(closeGap1.getLifeCycleId());
						if(fileNames != null && !fileNames.isEmpty()) {
							closeGap1.getUploadList().addAll(fileNames);
						}
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return closeGaps;
	}

	@Override
	public RestResult insertCloseGaps(CloseGaps closeGaps, String memberId, String measureId) {

		String sqlStatementInsert = "insert into qms_gic_lifecycle (member_id,quality_measure_id,interventions,"
				+ "priority,payor_comments,provider_comments,status,gap_date,"
				+ "curr_flag,rec_create_date,rec_update_date,latest_flag,active_flag,ingestion_date,source_name,"
				+ "user_name,gic_lifecycle_id,PRODUCT_PLAN_ID,HEDIS_GAPS_IN_CARE_SK,PATIENT_ID,TARGET_DATE,ACTION_ON_CARE_GAP) "
				+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		
		PreparedStatement statement = null;
		Connection connection = null;
		Statement statementObj = null;
		ResultSet resultSet = null;
		try {	
			connection = qmsConnection.getOracleConnection();
			
			statementObj = connection.createStatement();			
			resultSet = statementObj.executeQuery("select gap_date,status,HEDIS_GAPS_IN_CARE_SK,PRODUCT_PLAN_ID,user_name "
					+ "from qms_gic_lifecycle where member_id='"+memberId+"' and "
					+ "quality_measure_id = '"+measureId+"' order by gap_date desc");
			String productId = null;
			String hedisGapsSK = null;
			String userName = null;
			String status = null;
			if (resultSet.next()) {
				productId = resultSet.getString("PRODUCT_PLAN_ID");
				hedisGapsSK = resultSet.getString("HEDIS_GAPS_IN_CARE_SK");
				userName = resultSet.getString("user_name");
				status = resultSet.getString("status");
				System.out.println(productId + " :::: " + hedisGapsSK);
			}			
			
			resultSet.close();
			int lifeCycleId = 0;
			resultSet = statementObj.executeQuery("select max(gic_lifecycle_id) from qms_gic_lifecycle");
			while (resultSet.next()) {
				lifeCycleId = resultSet.getInt(1)+1;
			}
			resultSet.close();
			statement = connection.prepareStatement(sqlStatementInsert);
			Set<CloseGap> closeGapSet = closeGaps.getCareGaps();
			if(closeGapSet.isEmpty()) {
				return RestResult.getFailRestResult("Close Gap not found in Request body. ");
			}
			CloseGap closeGap = closeGapSet.iterator().next();
			int i=0;							
			statement.setString(++i, memberId);
			statement.setString(++i, measureId);
			statement.setString(++i, closeGap.getIntervention());
			statement.setString(++i, closeGap.getPriority());
			statement.setString(++i, closeGap.getPayerComments());
			statement.setString(++i, closeGap.getProviderComments());
			System.out.println(" User data --> " + userData);
			System.out.println(" User Role Id --> " + userData.getRoleId());
			String updateStatus = getStatus(userData.getRoleId(), status, closeGap.getCloseGap());
			if(updateStatus == null) {
				updateStatus = status;
			}
			statement.setString(++i, updateStatus);
			//statement.setString(++i, closeGap.getStatus());	
			
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
				statement.setString(++i, userName);
			
			statement.setInt(++i, lifeCycleId);
			statement.setString(++i, productId);
			statement.setString(++i, hedisGapsSK);
			statement.setString(++i, memberId);
			statement.setString(++i, closeGap.getTargetDate());
			statement.setString(++i, closeGap.getActionCareGap());
			statement.executeUpdate();
			httpSession.setAttribute(QMSConstants.SESSION_TYPE_ID, lifeCycleId+"");
			return RestResult.getSucessRestResult(" Close Gaps updation Success. ");
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statementObj, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}			
	}
	
	private String getStatus(String roleId, String status, String closeGap) throws Exception {
		String roleName = userRoleService.getRole(roleId);
		String updateStatus = null;
		System.out.println(roleName + " current status --> " + status);
		if(roleName.equalsIgnoreCase(QMSConstants.ROLE_PAYER)) {
			if(closeGap != null && (closeGap.equalsIgnoreCase("Y") || closeGap.equalsIgnoreCase("true")))
				updateStatus = "Close by Payer";
			else if(status.equalsIgnoreCase("Open")) 
				updateStatus = "Assigned to Provider";
			else if(status.equalsIgnoreCase("Review for Payer")) 
				updateStatus = "Close by Payer";
		} else if(roleName.equalsIgnoreCase(QMSConstants.ROLE_PHYSICIAN)) {
			if(status.equalsIgnoreCase("Assigned to Provider")) 
				updateStatus = "Review for Payer";
			else if(status.equalsIgnoreCase("Submitted by Nurse"))
				updateStatus = "Review for Payer";
				//updateStatus = "Assigned to provider";
		} else if(roleName.equalsIgnoreCase(QMSConstants.ROLE_NURSE)) {
			if(status.equalsIgnoreCase("Assigned to Provider")) 
				updateStatus = "Submitted by Nurse";
		} 
		System.out.println(roleName + " updating status to --> " + updateStatus);
		return null;
	}
	
	private String getPath (String type) {
		if(type.equalsIgnoreCase("close_gap"))
			return "D:/import_export/CLOSE_GAP/";
		if(type.equalsIgnoreCase("mit"))
			return "D:/import_export/MIT/";
		if(type.equalsIgnoreCase("persona"))
			return "D:/import_export/ME/PERSONA/";		
		return null;
	}

	@Override
	public RestResult importFile(MultipartFile uploadFile, String type) {
		try {
			String typeId = (String)httpSession.getAttribute(QMSConstants.SESSION_TYPE_ID);
			System.out.println(" TypeId from session --> " + typeId);
			if(typeId != null) {
				String path = getPath (type); //"D:/import_export/CLOSE_GAP/";
				GicLifeCycleFileUpload fileUpload = new GicLifeCycleFileUpload();
				fileUpload.setFileName(uploadFile.getOriginalFilename());
				fileUpload.setFilePath(path);
				fileUpload.setTypeId(typeId);
				fileUpload.setType(type);
				saveFileUpload(fileUpload);
				System.out.println(" QMSFileUpload DB success for typeId " + typeId);
				
				String parentDir = path+typeId;
				File parentDirFile = new File(parentDir);		
				if(!parentDirFile.exists()) {
					parentDirFile.mkdir();
					System.out.println(typeId+" upload dir created..");
				}
				
				FileOutputStream out = new FileOutputStream(parentDir+"/"+uploadFile.getOriginalFilename());
				out.write(uploadFile.getBytes());
				out.close();	
				System.out.println(" file created success in windows ..");
				httpSession.removeAttribute(QMSConstants.SESSION_TYPE_ID);
				return RestResult.getSucessRestResult(" File Upload Success.. ");			
			} else {
				return RestResult.getFailRestResult(" TypeId is null. Update "+type+" failed. ");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return RestResult.getFailRestResult(e.getMessage());
		} 
	}

	@Override
	public RestResult saveFileUpload(GicLifeCycleFileUpload fileUpload) {		
		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = null;
		Statement statementObj = null;
		ResultSet resultSet = null;
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		try {
			connection = qmsConnection.getOracleConnection();	
			statementObj = connection.createStatement();			
			
			resultSet = statementObj.executeQuery("select max(FILE_ID) from QMS_FILE_UPLOAD");
			int fileId = 0;
			while (resultSet.next()) {
				fileId = resultSet.getInt(1);
			}
			fileId = fileId+1;
			resultSet.close();			
			System.out.println(" Adding the file upload with file id --> " + fileId);
			
			String sqlStatementInsert = "insert into QMS_FILE_UPLOAD(FILE_ID,"
					+ "TYPE_ID,PATH,FILE_NAME,TYPE,"
					+ "curr_flag,rec_create_date,rec_update_date,latest_flag,active_flag,ingestion_date,source_name,user_name) "
					+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
			statement = connection.prepareStatement(sqlStatementInsert);
			int i=0;
			statement.setInt(++i, fileId);
			statement.setString(++i, fileUpload.getTypeId());
			statement.setString(++i, fileUpload.getFilePath());
			statement.setString(++i, fileUpload.getFileName());
			statement.setString(++i, fileUpload.getType());
			
			Date date = new Date();				
			Timestamp timestamp = new Timestamp(date.getTime());							
			
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
			restResult = RestResult.getSucessRestResult("Saving uploaded file Metadata in DB success.");
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
	public HashMap<Integer, List<String>> getUploadFileByTypeId (Statement statement, List<Integer> typeIds, 
			int typeId, String type) throws Exception {
		HashMap<Integer, List<String>> typeIdFileNameMap = null; 
		
		if(typeIds == null) {
			typeIds = new ArrayList<>();
			typeIds.add(typeId);
		}
		
		String glcIds = typeIds.toString().substring(1, (typeIds.toString().length()-1));
		ResultSet resultSet = statement.executeQuery("select TYPE_ID,PATH,FILE_NAME from "
				+ "QMS_FILE_UPLOAD where TYPE='"+type+"' AND TYPE_ID in("+glcIds+")");
		int typeIdLocal = 0;
		List<String> fileNames = null;
		while (resultSet.next()) {
			if(typeIdFileNameMap == null)
				typeIdFileNameMap = new HashMap<>();
			
			typeIdLocal = resultSet.getInt("TYPE_ID");
			fileNames = typeIdFileNameMap.get(typeIdLocal);
			if(fileNames == null) {
				fileNames = new ArrayList<>();
				typeIdFileNameMap.put(typeIdLocal, fileNames);
			} 
			fileNames.add(resultSet.getString("PATH")+resultSet.getString("TYPE_ID")+"/"+resultSet.getString("FILE_NAME"));			
		}		
		qmsConnection.closeJDBCResources(resultSet, null, null);
		return typeIdFileNameMap;
	}

}
 