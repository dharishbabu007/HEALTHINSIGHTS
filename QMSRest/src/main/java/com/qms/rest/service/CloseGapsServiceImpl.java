package com.qms.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.qms.rest.model.CloseGap;
import com.qms.rest.model.GicLifeCycleFileUpload;
import com.qms.rest.model.CloseGaps;
import com.qms.rest.model.FileUpload;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;

@Service("closeGapsService123")
public class CloseGapsServiceImpl implements CloseGapsService {
	
	@Autowired
	private QMSConnection qmsConnection;	
	
	@Autowired 
	private HttpSession httpSession;	

	@Override
	public CloseGaps getCloseGaps(String memberId, String measureId) {
		
		CloseGaps closeGaps = new CloseGaps();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getPhoenixConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select dm.gender, dm.member_id, (dm.FIRST_NAME||' '||dm.MIDDLE_NAME||' '||dm.LAST_NAME) AS NAME, dd.CALENDAR_DATE from QMS.dim_member dm, QMS.dim_date dd "
					+ "where dm.date_of_birth_sk=dd.date_sk and member_id="+memberId);
			if (resultSet.next()) {
				closeGaps.setGender(resultSet.getString("dm.gender"));
				closeGaps.setDateOfBirth(resultSet.getString("dd.CALENDAR_DATE"));
				closeGaps.setMemberId(resultSet.getString("dm.member_id"));
				closeGaps.setName(resultSet.getString("NAME"));
			}
			
			resultSet.close();	
			if(measureId == null || measureId.equalsIgnoreCase("0") || measureId.equalsIgnoreCase("all")) {
				resultSet = statement.executeQuery("select qgl.*, dqm.measure_title from QMS.qms_gic_lifecycle qgl, "
						+ "QMS.dim_quality_measure dqm where dqm.quality_measure_id = qgl.quality_measure_id and "
						+ "member_id="+memberId+" order by gap_date desc");
			} else {
				resultSet = statement.executeQuery("select qgl.*, dqm.measure_title from QMS.qms_gic_lifecycle qgl, "
						+ "QMS.dim_quality_measure dqm where dqm.quality_measure_id = qgl.quality_measure_id and "
						+ "qgl.quality_measure_id = "+measureId+" and  "
						+ "member_id="+memberId+" order by gap_date desc");				
			}
			CloseGap closeGap = null;
			Set<CloseGap> closeGapSet = new LinkedHashSet<>();
			while (resultSet.next()) {
				closeGap = new CloseGap();
				closeGap.setMeasureTitle(resultSet.getString("dqm.measure_title"));
				closeGap.setQualityMeasureId(resultSet.getString("qgl.quality_measure_id"));
				closeGap.setPayerComments(resultSet.getString("qgl.payor_comments"));
				closeGap.setProviderComments(resultSet.getString("qgl.provider_comments"));
				closeGap.setGapDate(resultSet.getString("qgl.gap_date"));
				closeGap.setIntervention(resultSet.getString("qgl.interventions"));
				closeGap.setPriority(resultSet.getString("qgl.priority"));
				closeGap.setStatus(resultSet.getString("qgl.status"));
				closeGapSet.add(closeGap);				
			}
			closeGaps.setCareGaps(closeGapSet);	
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
				+ "user_name,gic_lifecycle_id,PRODUCT_PLAN_ID,HEDIS_GAPS_IN_CARE_SK) "
				+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		
		PreparedStatement statement = null;
		Connection connection = null;
		Statement statementObj = null;
		ResultSet resultSet = null;
		try {	
			connection = qmsConnection.getOracleConnection();
			
			statementObj = connection.createStatement();			
			resultSet = statementObj.executeQuery("select HEDIS_GAPS_IN_CARE_SK,PRODUCT_PLAN_ID,user_name"
					+ " from qms_gic_lifecycle where member_id='"+memberId+"' and quality_measure_id = '"+measureId+"'");
			String productId = null;
			String hedisGapsSK = null;
			String userName = null;
			if (resultSet.next()) {
				productId = resultSet.getString("PRODUCT_PLAN_ID");
				hedisGapsSK = resultSet.getString("HEDIS_GAPS_IN_CARE_SK");
				userName = resultSet.getString("user_name");
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
			statement.setString(++i, closeGap.getStatus());	
			
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
			statement.executeUpdate();
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

	@Override
	public RestResult importFile(MultipartFile file, String type) {
		return null;
	}

	@Override
	public RestResult saveFileUpload(GicLifeCycleFileUpload fileUpload) {
		return null;
	}

	@Override
	public HashMap<Integer, List<String>> getUploadFileByTypeId(Statement statement, List<Integer> typeIds, int typeId,
			String type) throws Exception {
		return null;
	}

}
 