package com.qms.rest.service;

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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.MeasureCreator;
import com.qms.rest.model.NameValue;
import com.qms.rest.model.Param;
import com.qms.rest.model.RefMrss;
import com.qms.rest.model.RefMrssSample;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;
import com.qms.rest.util.QMSDateUtil;


@Service("qmsService")
public class QMSServiceImpl2 implements QMSService {
	
	@Autowired
	private QMSConnection qmsConnection;	
	
	@Autowired 
	private HttpSession httpSession;	
	
	@Override
	public Set<MeasureCreator> getMeasureLibrary(String programName, String value) {		
		Set<MeasureCreator> measureList = new LinkedHashSet<>();
		HashMap<String, String> programMap = getIdNameMap("QMS_QUALITY_PROGRAM", "QUALITY_PROGRAM_ID", "PROGRAM_NAME");
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		HashMap<String, String> stewardMap = getIdNameMap("QMS_MEASURE_STEWARD", "MEASURE_STEWARD_ID", "MEASURE_STEWARD_NAME");
		HashMap<String, String> statusMap = getIdNameMap("QMS_MEASURE_STATUS", "MEASURE_STATUS_ID", "MEASURE_STATUS_NAME");
		
		String whereClause = "where (MEASURE_STATUS_ID=5 or MEASURE_STATUS_ID=8)";
		if(programName.equalsIgnoreCase("Reimbursement")) {
			whereClause = " where (MEASURE_STATUS_ID=5 or MEASURE_STATUS_ID=8) and QUALITY_PROGRAM_ID in (select QUALITY_PROGRAM_ID from QMS_QUALITY_PROGRAM where PROGRAM_NAME='"+value+"')";
		}
		else if(programName.equalsIgnoreCase("Clinical")) {
			whereClause = " where (MEASURE_STATUS_ID=5 or MEASURE_STATUS_ID=8) and clinical_conditions='"+value+"'";													
		}
		else if(programName.equalsIgnoreCase("NQF")) {
			whereClause = " where (MEASURE_STATUS_ID=5 or MEASURE_STATUS_ID=8) and measure_domain_id='"+value+"'";
		}		

		String measureQuery = "select * from QMS_QUALITY_MEASURE "+whereClause+" order by QUALITY_MEASURE_ID asc, MEASURE_EDIT_ID desc";
		System.out.println("****measureQuery --> " + measureQuery);
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		Set<MeasureCreator> treeMeasureList = new TreeSet<>();
		Set<Integer> measureIdsAdded = new TreeSet<>();
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			//resultSet = statement.executeQuery("select * from QMS_QUALITY_MEASURE "+whereClause+" and IS_ACTIVE='Y' order by QUALITY_MEASURE_ID asc"); //0106
			resultSet = statement.executeQuery(measureQuery); 
			MeasureCreator measureCreator = null;
			int measureId = 0;
			while (resultSet.next()) {
				measureId = resultSet.getInt("QUALITY_MEASURE_ID");				
				if(measureIdsAdded.contains(measureId)) {
					continue;
				}
				measureIdsAdded.add(measureId);
				measureCreator = new MeasureCreator();
				measureCreator.setName(resultSet.getString("measure_name"));
				measureCreator.setClinocalCondition(resultSet.getString("clinical_conditions"));
				measureCreator.setId(measureId);
				measureCreator.setProgramName(programMap.get(resultSet.getString("QUALITY_PROGRAM_ID")));
				measureCreator.setSteward(stewardMap.get(resultSet.getString("measure_steward_id")));
				measureCreator.setType(typeMap.get(resultSet.getString("measure_type_id")));
				measureCreator.setIsActive(resultSet.getString("IS_ACTIVE"));
				measureCreator.setStartDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("START_DATE")));
				measureCreator.setEndDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("END_DATE")));				
				measureCreator.setStatus(statusMap.get(resultSet.getString("MEASURE_STATUS_ID")));
				measureList.add(measureCreator);
			}			
			
			treeMeasureList.addAll(measureList);
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}	
		
		return treeMeasureList;
	}

	
	@Override
	public MeasureCreator getMeasureLibraryById(int id) {
		
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		HashMap<String, String> domainMap = getIdNameMap("QMS_MEASURE_DOMAIN", "MEASURE_DOMAIN_ID", "MEASURE_DOMAIN_NAME");
		MeasureCreator measureCreator = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery("select qm.*,qqp.PROGRAM_NAME,qqp.CATEGORY_NAME from QMS_QUALITY_MEASURE qm, QMS_QUALITY_PROGRAM qqp where qm.QUALITY_MEASURE_ID='"+id+"' and qm.MEASURE_STATUS_ID='5' and qm.IS_ACTIVE='Y' and qm.QUALITY_PROGRAM_ID=qqp.QUALITY_PROGRAM_ID");			
			
			while (resultSet.next()) {
				measureCreator = new MeasureCreator();
				measureCreator.setClinocalCondition(resultSet.getString("clinical_conditions"));
				measureCreator.setDataSource(resultSet.getString("measure_source_id"));
				measureCreator.setDenominator(resultSet.getString("denominator"));
				measureCreator.setTarget(resultSet.getString("target")); 
				measureCreator.setMeasureDomain(domainMap.get(resultSet.getString("measure_domain_id")));
				measureCreator.setDenomExclusions(resultSet.getString("DENO_EXCLUSIONS"));
				measureCreator.setNumerator(resultSet.getString("numerator"));
				measureCreator.setNumeratorExclusions(resultSet.getString("num_exclusion"));				
				measureCreator.setId(resultSet.getInt("QUALITY_MEASURE_ID")); 
				measureCreator.setMeasureCategory(resultSet.getString("CATEGORY_NAME"));
				measureCreator.setDescription(resultSet.getString("description"));
				measureCreator.setName(resultSet.getString("measure_name"));
				measureCreator.setProgramName(resultSet.getString("PROGRAM_NAME"));
				measureCreator.setSteward(resultSet.getString("measure_steward_id"));			
				measureCreator.setTargetAge(resultSet.getString("target_population_age"));
				measureCreator.setType(typeMap.get(resultSet.getString("measure_type_id")));
				measureCreator.setMeasureEditId(resultSet.getInt("MEASURE_EDIT_ID"));
				measureCreator.setStatus(resultSet.getString("MEASURE_STATUS_ID"));
				measureCreator.setIsActive(resultSet.getString("IS_ACTIVE"));
				measureCreator.setStartDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("START_DATE")));
				measureCreator.setEndDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("END_DATE")));
				
				measureCreator.setP50(resultSet.getInt("P50"));
				measureCreator.setP90(resultSet.getInt("P90"));
				measureCreator.setCollectionSource(resultSet.getString("COLLECTION_SOURCE"));
				measureCreator.setMrss(resultSet.getInt("MRSS"));
				measureCreator.setOverFlowRate(resultSet.getString("OVERFLOW_RATE"));				
				//measureCreator.setCollectionMethod(resultSet.getString("OVERFLOW_RATE"));
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			

		return measureCreator;		
	}	
	
	
	@Override
	public Set<String> getQMSHomeDropDownList(String tableName, String columnName) {
		Set<String> dataSet = new HashSet<>();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();		
			if(columnName.equalsIgnoreCase("PROGRAM_NAME")) {
				resultSet = statement.executeQuery("select QQP.PROGRAM_NAME from QMS_QUALITY_MEASURE QM, QMS_QUALITY_PROGRAM QQP where QM.MEASURE_STATUS_ID='5' and QM.QUALITY_PROGRAM_ID=QQP.QUALITY_PROGRAM_ID");
			} else {
				resultSet = statement.executeQuery("select "+ columnName + " from "+tableName+" where MEASURE_STATUS_ID='5'");
			}
			
			String data = null;
			while (resultSet.next()) {
				data = resultSet.getString(1);
				if(data != null && !data.trim().isEmpty() && !data.equalsIgnoreCase("n/a") && !data.equalsIgnoreCase("#n/a"))
					dataSet.add(data);			
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
	public Set<NameValue> getMeasureDropDownNameValueList(String tableName, String columnValue, String columnName) {
		Set<NameValue> dataSet = new HashSet<>();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select distinct "+columnValue+","+columnName+" from "+tableName+" order by "+columnValue);
			
			String data = null;
			NameValue nameValue = null;
			while (resultSet.next()) {
				data = resultSet.getString(columnName);
				if(data != null && !data.trim().isEmpty() && !data.equalsIgnoreCase("n/a") && !data.equalsIgnoreCase("#n/a")) {
					nameValue = new NameValue();
					nameValue.setName(resultSet.getString(columnName));
					nameValue.setValue(resultSet.getString(columnValue));
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
	public Set<String> getMeasureDropDownList(String tableName, String columnName) {
		Set<String> dataSet = new HashSet<>();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from "+tableName);
			
			String data = null;
			while (resultSet.next()) {
				data = resultSet.getString(columnName);
				if(data != null && !data.trim().isEmpty() && !data.equalsIgnoreCase("n/a") && !data.equalsIgnoreCase("#n/a"))
					dataSet.add(data);			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		return dataSet;				
	}
	
	private String validateMeasureDates (MeasureCreator measureCreator) {
		
		if(measureCreator.getStartDate() == null && measureCreator.getEndDate() == null)
			return null;
		long measureStartDate = QMSDateUtil.getDateInLong(measureCreator.getStartDate(), null);
		long measureEndDate = QMSDateUtil.getDateInLong(measureCreator.getEndDate(), null);
		if(measureStartDate > measureEndDate) {
			return "Invalid Measure Start and End Dates.";
		}

		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select START_DATE,END_DATE from "
					+ "QMS_QUALITY_PROGRAM where PROGRAM_NAME = '"+measureCreator.getProgramName()+"'");
			if (resultSet.next()) {

				if(resultSet.getString("START_DATE") != null && measureCreator.getStartDate() != null) {
					if(measureStartDate < QMSDateUtil.getDateInLong(QMSDateUtil.getSQLDateFormat(resultSet.getDate("START_DATE")), null)) {
						return "Measure Start Date should match with Program Start Date. "
								+ "It should be on/after "+QMSDateUtil.getSQLDateFormat(resultSet.getDate("START_DATE"))+".";
						
					}
				}

				if(resultSet.getString("END_DATE") != null && measureCreator.getEndDate() != null) {					
					if(measureEndDate > QMSDateUtil.getDateInLong(QMSDateUtil.getSQLDateFormat(resultSet.getDate("END_DATE")), null)) {
						return "Measure End Date should match with Program End Date. "
								+ "It should be on/before "+QMSDateUtil.getSQLDateFormat(resultSet.getDate("END_DATE")) +".";
					}
				}				
			}			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		

		return null;
	}

	@Override
	public RestResult insertMeasureCreator(MeasureCreator measureCreator) {		
		
		String errorMessage = validateMeasureDates(measureCreator);
		if(errorMessage != null) {
			return RestResult.getFailRestResult(errorMessage);
		}
		
		HashMap<String, String> statusMap = getIdNameMap("QMS_MEASURE_STATUS", "MEASURE_STATUS_ID", "MEASURE_STATUS_NAME");
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		HashMap<String, String> domainMap = getIdNameMap("QMS_MEASURE_DOMAIN", "MEASURE_DOMAIN_ID", "MEASURE_DOMAIN_NAME");		
		String qualityProgramId = getQualityProgramId(measureCreator.getProgramName(), measureCreator.getMeasureCategory());
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);

		String sqlStatementInsert = 
				"insert into QMS_QUALITY_MEASURE (clinical_conditions,measure_source_id,denominator,target,measure_domain_id,deno_exclusions,"
				+ "numerator,num_exclusion,QUALITY_MEASURE_ID,description,measure_name,QUALITY_PROGRAM_ID,target_population_age,"
				+ "measure_type_id,measure_edit_id,REC_UPDATE_DATE,MEASURE_STATUS_ID,ACTIVE_FLAG,REVIEWER_ID,AUTHOR_ID,USER_NAME,IS_ACTIVE,"
				+ "START_DATE,END_DATE,curr_flag,rec_create_date,latest_flag,ingestion_date,source_name,"
				+ "P50,P90,COLLECTION_SOURCE,MRSS,OVERFLOW_RATE,review_comments) "
				+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement statement = null;
		Statement sqlStatement = null;
		Connection connection = null;
		RestResult restResult = new RestResult();
		ResultSet resultSet = null;
		try {	

			connection = qmsConnection.getOracleConnection();
			connection.setAutoCommit(false);
			sqlStatement = connection.createStatement();
			
			int measureId = 0;
			//to get the last created measure id			
			if(measureCreator.getMeasureEditId() == 0) {
				resultSet = sqlStatement.executeQuery("select max(QUALITY_MEASURE_ID) from QMS_QUALITY_MEASURE");
				while (resultSet.next()) {
					measureId = resultSet.getInt(1)+1;
				}
			}
			
			String isActive = "N";
			if(measureCreator.getStatus() != null && measureCreator.getStatus().equalsIgnoreCase("Approved")) {
				String sqlStatementUpdate = 
						"update QMS_QUALITY_MEASURE set IS_ACTIVE='N' where QUALITY_MEASURE_ID="+measureCreator.getId()+" and measure_edit_id<>"+measureCreator.getMeasureEditId();
				sqlStatement.executeUpdate(sqlStatementUpdate);
				isActive = "Y";
			}			
			
			int i=0;			
			statement = connection.prepareStatement(sqlStatementInsert);
			statement.setString(++i, measureCreator.getClinocalCondition());
			statement.setString(++i, measureCreator.getDataSource());
			statement.setString(++i, measureCreator.getDenominator());
			statement.setString(++i, measureCreator.getTarget());
			statement.setString(++i, domainMap.get(measureCreator.getMeasureDomain()));
			
			statement.setString(++i, measureCreator.getDenomExclusions());
			
			statement.setString(++i, measureCreator.getNumerator());
			statement.setString(++i, measureCreator.getNumeratorExclusions());
			
			//first version
			System.out.println(" measureCreator.getMeasureEditId() --> " + measureCreator.getMeasureEditId());
			if(measureCreator.getMeasureEditId() == 0) {
				System.out.println(" Creating the measure with id --> " + measureId);				
				statement.setString(++i, measureId+"");
				measureCreator.setId(measureId);
			} else {
				statement.setInt(++i, measureCreator.getId());
			}
			
			statement.setString(++i, measureCreator.getDescription());
			statement.setString(++i, measureCreator.getName());
			statement.setString(++i, qualityProgramId);
			
			statement.setString(++i, measureCreator.getTargetAge());
			statement.setString(++i, typeMap.get(measureCreator.getType()));	
			
			if(measureCreator.getMeasureEditId() == 0)
				statement.setInt(++i, 1); //version
			else 
				statement.setInt(++i, measureCreator.getMeasureEditId()); //version
			
			Date date = new Date();
			Timestamp timestamp = new Timestamp(date.getTime());
			statement.setTimestamp(++i, timestamp);			
			statement.setString(++i, statusMap.get(measureCreator.getStatus()==null?QMSConstants.MEASURE_DEFAULT_STATUS:measureCreator.getStatus()));
			statement.setString(++i, isActive);
			statement.setString(++i, QMSConstants.MEASURE_REVIEWER_ID);
			statement.setString(++i, QMSConstants.MEASURE_REVIEWER_ID);
			if(userData == null || userData.getId() == null)
				statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
			else
				statement.setString(++i, userData.getId());
			//IS_ACTIVE
			if(measureCreator.getIsActive()!=null && measureCreator.getIsActive().equalsIgnoreCase("N"))
				statement.setString(++i, "N");
			else
				statement.setString(++i, "Y");
			
			statement.setString(++i, measureCreator.getStartDate());
			statement.setString(++i, measureCreator.getEndDate());
			statement.setString(++i, "Y");
			statement.setTimestamp(++i, timestamp);
			statement.setString(++i, "Y");
			statement.setTimestamp(++i, timestamp);	
			statement.setString(++i, QMSConstants.MEASURE_SOURCE_NAME);
			
			statement.setInt(++i, measureCreator.getP50());
			statement.setInt(++i, measureCreator.getP90());
			statement.setString(++i, measureCreator.getCollectionSource());
			statement.setInt(++i, measureCreator.getMrss());
			statement.setString(++i, measureCreator.getOverFlowRate());			
			statement.setString(++i, measureCreator.getReviewComments());
			
			statement.executeUpdate();
			connection.commit();
			System.out.println("Added the measure worklist with id --> " + measureCreator.getId() + " status --> " + 
					measureCreator.getStatus() + " version --> " + measureCreator.getMeasureEditId());
			restResult.setStatus(RestResult.SUCCESS_STATUS);
			restResult.setMessage(" New measure creation. ");
		} catch (Exception e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			restResult.setStatus(RestResult.FAIL_STATUS);
			if(e.getMessage().contains("QUALITY_PROGRAM_ID"))
				restResult.setMessage("Invalid Program Name and Measure Category mapping. ");
//			else if(e.getMessage().contains("invalid number"))
//				restResult.setMessage("Target should be a number. ");
			else
				restResult.setMessage(e.getMessage());
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, sqlStatement, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}	
		return restResult;
	}

	@Override
	public RestResult updateMeasureCreator(MeasureCreator measureCreator) {		
		HashMap<String, String> statusMap = getIdNameMap("QMS_MEASURE_STATUS", "MEASURE_STATUS_ID", "MEASURE_STATUS_NAME");
		
		//new version create		
		MeasureCreator lastCreatedCreator = null;
		lastCreatedCreator = findMeasureCreatorById(measureCreator.getId());
		
		String lastStatusId = lastCreatedCreator.getStatus();
		String currentStatusId = statusMap.get(measureCreator.getStatus()==null?QMSConstants.MEASURE_DEFAULT_STATUS:measureCreator.getStatus());
		System.out.println(" lastStatusId --> " + lastStatusId + " currentStatusId --> " + currentStatusId);
		if(!lastStatusId.equalsIgnoreCase(currentStatusId)) {
			int currentVersion = lastCreatedCreator.getMeasureEditId()+1;
			measureCreator.setMeasureEditId(currentVersion);
			System.out.println(" Creating the MeasureCreator with version --> " + currentVersion + " for id --> " + measureCreator.getId());
			return this.insertMeasureCreator(measureCreator);
		}
		
		System.out.println(" Updating the record for id --> " + measureCreator.getId() + " edit id --> " + lastCreatedCreator.getMeasureEditId());
		
		String errorMessage = validateMeasureDates(measureCreator);
		if(errorMessage != null) {
			return RestResult.getFailRestResult(errorMessage);
		}		
		
		String sqlStatementUpdate = 
				"update QMS_QUALITY_MEASURE set clinical_conditions=?, measure_source_id=?, denominator=?, target=?, "
				+ "measure_domain_id=?, deno_exclusions=?, numerator=?, num_exclusion=?, description=?, measure_name=?, "
				+ "QUALITY_PROGRAM_ID=?, measure_steward_id=?, target_population_age=?, measure_type_id=?, rec_update_date=?, "
				+ "MEASURE_STATUS_ID=?, USER_NAME=?, IS_ACTIVE=?, START_DATE=?, END_DATE=?, "
				+ "P50=?,P90=?,COLLECTION_SOURCE=?,MRSS=?,OVERFLOW_RATE=? "
				+ "where QUALITY_MEASURE_ID=? and MEASURE_EDIT_ID=?";
		
		String qualityProgramId = this.getQualityProgramId(measureCreator.getProgramName(), measureCreator.getMeasureCategory());
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		HashMap<String, String> domainMap = getIdNameMap("QMS_MEASURE_DOMAIN", "MEASURE_DOMAIN_ID", "MEASURE_DOMAIN_NAME");
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		
		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = new RestResult();
		try {					
			int i=0;
			connection = qmsConnection.getOracleConnection();
			statement = connection.prepareStatement(sqlStatementUpdate);
			statement.setString(++i, measureCreator.getClinocalCondition());
			statement.setString(++i, measureCreator.getDataSource());
			statement.setString(++i, measureCreator.getDenominator());
			statement.setString(++i, measureCreator.getTarget()); 
			
			//statement.setString(++i, measureCreator.getMeasureDomain());
			statement.setString(++i, domainMap.get(measureCreator.getMeasureDomain()));
			statement.setString(++i, measureCreator.getDenomExclusions());
			statement.setString(++i, measureCreator.getNumerator());
			statement.setString(++i, measureCreator.getNumeratorExclusions());	
			statement.setString(++i, measureCreator.getDescription());
			statement.setString(++i, measureCreator.getName());
			
			statement.setString(++i, qualityProgramId);
			statement.setString(++i, measureCreator.getSteward());			
			statement.setString(++i, measureCreator.getTargetAge());
			statement.setString(++i, typeMap.get(measureCreator.getType()));			
			Date date = new Date();
			statement.setTimestamp(++i, new Timestamp(date.getTime()));
			statement.setString(++i, statusMap.get(measureCreator.getStatus()==null?QMSConstants.MEASURE_DEFAULT_STATUS:measureCreator.getStatus()));
			if(userData != null && userData.getId() != null)
				statement.setString(++i, userData.getId());
			else
				statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
			//IS_ACTIVE
			if(measureCreator.getIsActive()!=null && measureCreator.getIsActive().equalsIgnoreCase("N"))
				statement.setString(++i, "N");
			else
				statement.setString(++i, "Y");	
			
			statement.setString(++i, measureCreator.getStartDate());
			statement.setString(++i, measureCreator.getEndDate());
			
			statement.setInt(++i, measureCreator.getP50());
			statement.setInt(++i, measureCreator.getP90());
			statement.setString(++i, measureCreator.getCollectionSource());
			statement.setInt(++i, measureCreator.getMrss());
			statement.setString(++i, measureCreator.getOverFlowRate());			
			
			//where clause parameters
			statement.setInt(++i, measureCreator.getId());
			statement.setInt(++i, lastCreatedCreator.getMeasureEditId());
			
			statement.executeUpdate();
			
			restResult.setStatus(RestResult.SUCCESS_STATUS);
			restResult.setMessage(" update measure. ");			
			System.out.println("Updated the measure worklist with id --> " + measureCreator.getId());
		} catch (Exception e) {			
			e.printStackTrace();
			restResult.setStatus(RestResult.FAIL_STATUS);
			if(e.getMessage().contains("QUALITY_PROGRAM_ID"))
				//restResult.setMessage("Invalid Program Name and Measure Category mapping. ");
				restResult.setMessage("Invalid Measure Category for selected Program Name. ");
				//restResult.setMessage("Select correct Measure Category for Program Name. ");
//			else if(e.getMessage().contains("invalid number"))
//				restResult.setMessage("Target should be a number. ");
			else
				restResult.setMessage(e.getMessage());			
		}
		finally {
			qmsConnection.closeJDBCResources(null, statement, connection);
		}
		return restResult;
	}

	@Override
	public MeasureCreator findMeasureCreatorById(int id) {
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		HashMap<String, String> domainMap = getIdNameMap("QMS_MEASURE_DOMAIN", "MEASURE_DOMAIN_ID", "MEASURE_DOMAIN_NAME");
		MeasureCreator measureCreator = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			
			resultSet = statement.executeQuery("select qm.*,qqp.PROGRAM_NAME,qqp.CATEGORY_NAME from QMS_QUALITY_MEASURE qm, QMS_QUALITY_PROGRAM qqp where qm.QUALITY_MEASURE_ID="+id+" and qm.QUALITY_PROGRAM_ID=qqp.QUALITY_PROGRAM_ID order by MEASURE_EDIT_ID desc");
			
			while (resultSet.next()) {
				measureCreator = new MeasureCreator();
				measureCreator.setClinocalCondition(resultSet.getString("clinical_conditions"));
				measureCreator.setDataSource(resultSet.getString("measure_source_id"));
				measureCreator.setDenominator(resultSet.getString("denominator"));
				measureCreator.setTarget(resultSet.getString("target")); 
				measureCreator.setMeasureDomain(domainMap.get(resultSet.getString("measure_domain_id")));
				measureCreator.setDenomExclusions(resultSet.getString("DENO_EXCLUSIONS"));
				measureCreator.setNumerator(resultSet.getString("numerator"));
				measureCreator.setNumeratorExclusions(resultSet.getString("num_exclusion"));				
				measureCreator.setId(resultSet.getInt("QUALITY_MEASURE_ID")); 
				measureCreator.setMeasureCategory(resultSet.getString("CATEGORY_NAME"));
				measureCreator.setDescription(resultSet.getString("description"));
				measureCreator.setName(resultSet.getString("measure_name"));
				measureCreator.setProgramName(resultSet.getString("PROGRAM_NAME"));
				measureCreator.setSteward(resultSet.getString("measure_steward_id"));			
				measureCreator.setTargetAge(resultSet.getString("target_population_age"));
				measureCreator.setType(typeMap.get(resultSet.getString("measure_type_id")));
				measureCreator.setMeasureEditId(resultSet.getInt("MEASURE_EDIT_ID"));
				measureCreator.setStatus(resultSet.getString("MEASURE_STATUS_ID"));
				measureCreator.setIsActive(resultSet.getString("IS_ACTIVE"));
				measureCreator.setStartDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("START_DATE")));
				measureCreator.setEndDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("END_DATE")));

				measureCreator.setP50(resultSet.getInt("P50"));
				measureCreator.setP90(resultSet.getInt("P90"));
				measureCreator.setCollectionSource(resultSet.getString("COLLECTION_SOURCE"));
				measureCreator.setMrss(resultSet.getInt("MRSS"));
				measureCreator.setOverFlowRate(resultSet.getString("OVERFLOW_RATE"));
				//measureCreator.setCollectionMethod(resultSet.getString("OVERFLOW_RATE"));
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			

		return measureCreator;
	}
	
	
	@Override
	public Set<MeasureCreator> getAllWorkList() {
		Set<MeasureCreator> dataSet = new LinkedHashSet<>();
		HashMap<String, String> statusMap = getIdNameMap("QMS_MEASURE_STATUS", "MEASURE_STATUS_ID", "MEASURE_STATUS_NAME");
		HashMap<String, String> userMap = getIdNameMap("QMS_USER_MASTER", "USER_ID", "USER_NAME");
		HashMap<String, String> programMap = getIdNameMap("QMS_QUALITY_PROGRAM", "QUALITY_PROGRAM_ID", "PROGRAM_NAME");
		MeasureCreator measureCreator = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		List<Integer> uniqueMeasureId = new ArrayList<>();  
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from QMS_QUALITY_MEASURE order by REC_UPDATE_DATE desc");
			int measureId = 0;
			while (resultSet.next()) {
				measureId = resultSet.getInt("QUALITY_MEASURE_ID");
				
				if(uniqueMeasureId.contains(measureId)) {
					continue;
				} else {
					uniqueMeasureId.add(measureId);	
					measureCreator = new MeasureCreator();					
					measureCreator.setId(measureId);
					measureCreator.setName(resultSet.getString("measure_name"));
					measureCreator.setProgramName(programMap.get(resultSet.getString("QUALITY_PROGRAM_ID")));
					measureCreator.setStatus(statusMap.get(resultSet.getString("MEASURE_STATUS_ID")));
					measureCreator.setReviewComments(resultSet.getString("review_comments"));
					measureCreator.setReviewedBy(userMap.get(resultSet.getString("REVIEWER_ID")));
					measureCreator.setIsActive(resultSet.getString("IS_ACTIVE"));
					measureCreator.setStartDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("START_DATE")));
					measureCreator.setEndDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("END_DATE")));					
					dataSet.add(measureCreator);
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
	
	private HashMap<String, String> getIdNameMap(String tableName, String idColumn, String nameColumn) {
		HashMap<String, String> statusMap = new HashMap<>();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from "+tableName);
			while (resultSet.next()) {

				statusMap.put(resultSet.getString(idColumn), resultSet.getString(nameColumn));
				statusMap.put(resultSet.getString(nameColumn), resultSet.getString(idColumn));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		return statusMap;				
	}
	
	

	@Override
	public RestResult updateMeasureWorkListStatus(int id, String status, Param param) {
		System.out.println("SERVICE Update Measure WorkList Status for id : " + id + " with status : " + status);		
		HashMap<String, String> statusMap = getIdNameMap("QMS_MEASURE_STATUS", "MEASURE_STATUS_ID", "MEASURE_STATUS_NAME");
		
		//new version create
		MeasureCreator lastCreatedCreator = findMeasureCreatorById(id);
		String lastStatusId = lastCreatedCreator.getStatus();
		String currentStatusId = statusMap.get(status);
		System.out.println(" lastStatusId --> " + lastStatusId + " currentStatusId --> " + currentStatusId);
		if(!lastStatusId.equalsIgnoreCase(currentStatusId)) {
			int currentVersion = lastCreatedCreator.getMeasureEditId()+1;
			lastCreatedCreator.setMeasureEditId(currentVersion);
			lastCreatedCreator.setStatus(status);
			lastCreatedCreator.setReviewComments(param.getValue1());
			System.out.println(" Creating the MeasureCreator with version --> " + currentVersion + " for id --> " + lastCreatedCreator.getId());
			return insertMeasureCreator(lastCreatedCreator);			
		} else {
			System.out.println(" No need to update the status as current status and selected status are same for id --> " + id);
			return RestResult.getFailRestResult("Current status and selected status is same. ");
		}
	}
	
	private String getQualityProgramId(String programId, String categoryName) {
		String qualityProgramId = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();	
			if(categoryName != null) {
				resultSet = statement.executeQuery("select QUALITY_PROGRAM_ID from QMS_QUALITY_PROGRAM where PROGRAM_NAME='"+programId+"' and CATEGORY_NAME='"+categoryName+"'");
				if (resultSet.next()) {
					qualityProgramId = resultSet.getString("QUALITY_PROGRAM_ID");				
				}
			}
			if(qualityProgramId == null) {
				if(resultSet != null) resultSet.close();
				resultSet = statement.executeQuery("select QUALITY_PROGRAM_ID from QMS_QUALITY_PROGRAM where PROGRAM_NAME='"+programId+"'");
				if (resultSet.next()) {
					qualityProgramId = resultSet.getString("QUALITY_PROGRAM_ID");				
				}				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		System.out.println(" getQualityProgramId " + programId + " categoryName " + categoryName + " qualityProgramId " + qualityProgramId);
		return qualityProgramId;
	}


	@Override
	public Set<String> getCategoryByProgramId(String programId) {
		Set<String> categorySet = new HashSet<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select CATEGORY_NAME from QMS_QUALITY_PROGRAM where PROGRAM_ID="+programId);
			
			while (resultSet.next()) {
				categorySet.add(resultSet.getString("CATEGORY_NAME"));				
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
	public Set<RefMrss> getRefMrssList() {
		
		Set<RefMrss> dataSet1 = new HashSet<>();
		
		RefMrss refMrss = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery("select * from QMS_REF_MRSS");			
			
			while (resultSet.next()) {
				 refMrss = new RefMrss();
				 refMrss.setMrssId(resultSet.getString("MRSS_ID"));
					refMrss.setMeasureName(resultSet.getString("MEASURE_NAME"));
					refMrss.setMeasureCategary(resultSet.getString("MEASURE_CATEGORY"));
					refMrss.setMedicaid(resultSet.getString("MEDICAID"));
					refMrss.setCommercial(resultSet.getString("COMMERCIAL"));
					refMrss.setMedicare(resultSet.getString("MEDICARE"));
					refMrss.setSample_size(resultSet.getString("SAMPLE_SIZE"));
					refMrss.setRand(resultSet.getString("RAND"));	
					dataSet1.add(refMrss);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return dataSet1;		
	}	
	

	
	@Override
	public Set<RefMrssSample> getRefMrssSaimpleList() {
		
		Set<RefMrssSample> dataSet2 = new HashSet<>();
	
		RefMrssSample refMrssSample = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery("select * from QMS_REF_MRSS_SAMPLE");			

			while (resultSet.next()) {
				refMrssSample = new RefMrssSample();
				refMrssSample.setMrssSampleId(resultSet.getString("MRSS_SAMPLE_ID"));
				refMrssSample.setRate(resultSet.getString("RATE"));
				refMrssSample.setSampleSize(resultSet.getString("SAMPLE_SIZE"));
				dataSet2.add(refMrssSample);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		return dataSet2;		
	}	

}
