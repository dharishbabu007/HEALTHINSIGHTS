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
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;


@Service("qmsService")
public class QMSServiceImpl implements QMSService {
	
	@Autowired
	private QMSConnection qmsConnection;	
	
	@Autowired 
	private HttpSession httpSession;	
	
	@Override
	public Set<MeasureCreator> getMeasureLibrary(String programName, String value) {		
		Set<MeasureCreator> measureList = new LinkedHashSet<>();
		HashMap<String, String> programMap = getIdNameMap("QMS_QUALITY_PROGRAM", "QUALITY_PROGRAM_ID", "PROGRAM_NAME");
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		HashMap<String, String> stewardMap = getIdNameMap("QMS_MEASURE_STEWARD", "STEWARD_TYPE_ID", "STEWARD_NAME");
		HashMap<String, String> statusMap = getIdNameMap("QMS_MEASURE_STATUS", "MEASURE_STATUS_ID", "MEASURE_STATUS_NAME");
		
		String whereClause = "where (STATUS_ID=5 or STATUS_ID=8)";
		if(programName.equalsIgnoreCase("Reimbursement")) {
			whereClause = " where (STATUS_ID=5 or STATUS_ID=8) and QUALITY_PROGRAM_ID in (select QUALITY_PROGRAM_ID from QMS_QUALITY_PROGRAM where PROGRAM_NAME='"+value+"')";
		}
		else if(programName.equalsIgnoreCase("Clinical")) {
			whereClause = " where (STATUS_ID=5 or STATUS_ID=8) and clinical_conditions='"+value+"'";													
		}
		else if(programName.equalsIgnoreCase("NQF")) {
			whereClause = " where (STATUS_ID=5 or STATUS_ID=8) and domain_id='"+value+"'";
		}		

		String measureQuery = "select * from QMS_MEASURE "+whereClause+" order by MEASURE_ID asc, MEASURE_EDIT_ID desc";
		System.out.println("****measureQuery --> " + measureQuery);
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		Set<MeasureCreator> treeMeasureList = new TreeSet<>();
		Set<Integer> measureIdsAdded = new TreeSet<>();
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			//resultSet = statement.executeQuery("select * from QMS_MEASURE "+whereClause+" and IS_ACTIVE='Y' order by MEASURE_ID asc"); //0106
			resultSet = statement.executeQuery(measureQuery); 
			MeasureCreator measureCreator = null;
			int measureId = 0;
			while (resultSet.next()) {
				measureId = resultSet.getInt("measure_id");				
				if(measureIdsAdded.contains(measureId)) {
					continue;
				}
				measureIdsAdded.add(measureId);
				measureCreator = new MeasureCreator();
				measureCreator.setName(resultSet.getString("measure_name"));
				measureCreator.setClinocalCondition(resultSet.getString("clinical_conditions"));
				measureCreator.setId(measureId);
				measureCreator.setProgramName(programMap.get(resultSet.getString("QUALITY_PROGRAM_ID")));
				measureCreator.setSteward(stewardMap.get(resultSet.getString("STEWARD_ID")));
				measureCreator.setType(typeMap.get(resultSet.getString("TYPE_ID")));
				measureCreator.setIsActive(resultSet.getString("IS_ACTIVE"));
				measureCreator.setStartDate(resultSet.getString("START_DATE"));
				measureCreator.setEndDate(resultSet.getString("END_DATE"));		
				measureCreator.setStatus(statusMap.get(resultSet.getString("status_id")));
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
			resultSet = statement.executeQuery("select qm.*,qqp.PROGRAM_NAME,qqp.CATEGORY_NAME from qms_measure qm, QMS_QUALITY_PROGRAM qqp where qm.measure_id='"+id+"' and qm.STATUS_ID='5' and qm.IS_ACTIVE='Y' and qm.QUALITY_PROGRAM_ID=qqp.QUALITY_PROGRAM_ID");			
			
			while (resultSet.next()) {
				measureCreator = new MeasureCreator();
				measureCreator.setClinocalCondition(resultSet.getString("clinical_conditions"));
				measureCreator.setDataSource(resultSet.getString("data_sources_id"));
				measureCreator.setDenominator(resultSet.getString("denominator"));
				measureCreator.setTarget(resultSet.getString("target")); 
				measureCreator.setMeasureDomain(domainMap.get(resultSet.getString("domain_id")));
				measureCreator.setDenomExclusions(resultSet.getString("DENO_EXCLUSIONS"));
				measureCreator.setNumerator(resultSet.getString("numerator"));
				measureCreator.setNumeratorExclusions(resultSet.getString("num_exclusion"));				
				measureCreator.setId(resultSet.getInt("measure_id")); 
				measureCreator.setMeasureCategory(resultSet.getString("CATEGORY_NAME"));
				measureCreator.setDescription(resultSet.getString("description"));
				measureCreator.setName(resultSet.getString("measure_name"));
				measureCreator.setProgramName(resultSet.getString("PROGRAM_NAME"));
				measureCreator.setSteward(resultSet.getString("steward_id"));			
				measureCreator.setTargetAge(resultSet.getString("target_population_age"));
				measureCreator.setType(typeMap.get(resultSet.getString("type_id")));
				measureCreator.setMeasureEditId(resultSet.getInt("MEASURE_EDIT_ID"));
				measureCreator.setStatus(resultSet.getString("STATUS_ID"));
				measureCreator.setIsActive(resultSet.getString("IS_ACTIVE"));
				measureCreator.setStartDate(resultSet.getString("START_DATE"));
				measureCreator.setEndDate(resultSet.getString("END_DATE"));
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
				resultSet = statement.executeQuery("select QQP.PROGRAM_NAME from QMS_MEASURE QM, QMS_QUALITY_PROGRAM QQP where QM.STATUS_ID='5' and QM.QUALITY_PROGRAM_ID=QQP.QUALITY_PROGRAM_ID");
			} else {
				resultSet = statement.executeQuery("select "+ columnName + " from "+tableName+" where STATUS_ID='5'");
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

	@Override
	public RestResult insertMeasureCreator(MeasureCreator measureCreator) {		
		HashMap<String, String> statusMap = getIdNameMap("QMS_MEASURE_STATUS", "MEASURE_STATUS_ID", "MEASURE_STATUS_NAME");
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		HashMap<String, String> domainMap = getIdNameMap("QMS_MEASURE_DOMAIN", "MEASURE_DOMAIN_ID", "MEASURE_DOMAIN_NAME");		
		String qualityProgramId = this.getQualityProgramId(measureCreator.getProgramName(), measureCreator.getMeasureCategory());
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);

		String sqlStatementInsert = 
				"insert into qms_measure (clinical_conditions,data_sources_id,denominator,target,domain_id,deno_exclusions,"
				+ "numerator,num_exclusion,measure_id,description,measure_name,QUALITY_PROGRAM_ID,target_population_age,"
				+ "type_id,measure_edit_id,REC_UPDATE_DATE,STATUS_ID,ACTIVE_FLAG,REVIEWER_ID,AUTHOR_ID,USER_NAME,IS_ACTIVE,"
				+ "START_DATE,END_DATE,curr_flag,rec_create_date,latest_flag,ingestion_date,source_name) "
				+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
				resultSet = sqlStatement.executeQuery("select max(measure_id) from qms_measure");
				while (resultSet.next()) {
					measureId = resultSet.getInt(1)+1;
				}
			}
			
			String isActive = "N";
			if(measureCreator.getStatus() != null && measureCreator.getStatus().equalsIgnoreCase("Approved")) {
				String sqlStatementUpdate = 
						"update qms_measure set IS_ACTIVE='N' where measure_id="+measureCreator.getId()+" and measure_edit_id<>"+measureCreator.getMeasureEditId();
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
			System.out.println(measureCreator.getStatus() + " KKKKKKKKKKKKstatus " + statusMap.get(measureCreator.getStatus()==null?"Open":measureCreator.getStatus()));
			statement.setString(++i, statusMap.get(measureCreator.getStatus()==null?"Open":measureCreator.getStatus()));
			statement.setString(++i, isActive);
			statement.setString(++i, "2");
			statement.setString(++i, "2");
			if(userData == null || userData.getId() == null)
				statement.setString(++i, "Raghu");
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
			statement.setString(++i, "UI");
			
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
			else if(e.getMessage().contains("invalid number"))
				restResult.setMessage("Target should be a number. ");
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
		String currentStatusId = statusMap.get(measureCreator.getStatus()==null?"Open":measureCreator.getStatus());
		System.out.println(" lastStatusId --> " + lastStatusId + " currentStatusId --> " + currentStatusId);
		if(!lastStatusId.equalsIgnoreCase(currentStatusId)) {
			int currentVersion = lastCreatedCreator.getMeasureEditId()+1;
			measureCreator.setMeasureEditId(currentVersion);
			System.out.println(" Creating the MeasureCreator with version --> " + currentVersion + " for id --> " + measureCreator.getId());
			return this.insertMeasureCreator(measureCreator);
		}
		
		System.out.println(" Updating the record for id --> " + measureCreator.getId() + " edit id --> " + lastCreatedCreator.getMeasureEditId());
		
		String sqlStatementUpdate = 
				"update qms_measure set clinical_conditions=?, data_sources_id=?, denominator=?, target=?, "
				+ "domain_id=?, deno_exclusions=?, numerator=?, num_exclusion=?, description=?, measure_name=?, "
				+ "QUALITY_PROGRAM_ID=?, STEWARD_ID=?, target_population_age=?, type_id=?, rec_update_date=?,  STATUS_ID=?,  USER_NAME=?, IS_ACTIVE=? "
				+ "where measure_id=? and MEASURE_EDIT_ID=?";
		
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
			statement.setString(++i, statusMap.get(measureCreator.getStatus()==null?"Open":measureCreator.getStatus()));
			if(userData != null && userData.getId() != null)
				statement.setString(++i, userData.getId());
			else
				statement.setString(++i, "Raghu");
			//IS_ACTIVE
			if(measureCreator.getIsActive()!=null && measureCreator.getIsActive().equalsIgnoreCase("N"))
				statement.setString(++i, "N");
			else
				statement.setString(++i, "Y");			
			
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
			else if(e.getMessage().contains("invalid number"))
				restResult.setMessage("Target should be a number. ");
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
			
			resultSet = statement.executeQuery("select qm.*,qqp.PROGRAM_NAME,qqp.CATEGORY_NAME from qms_measure qm, QMS_QUALITY_PROGRAM qqp where qm.measure_id="+id+" and qm.QUALITY_PROGRAM_ID=qqp.QUALITY_PROGRAM_ID order by MEASURE_EDIT_ID desc");
			
			while (resultSet.next()) {
				measureCreator = new MeasureCreator();
				measureCreator.setClinocalCondition(resultSet.getString("clinical_conditions"));
				measureCreator.setDataSource(resultSet.getString("data_sources_id"));
				measureCreator.setDenominator(resultSet.getString("denominator"));
				measureCreator.setTarget(resultSet.getString("target")); 
				measureCreator.setMeasureDomain(domainMap.get(resultSet.getString("domain_id")));
				measureCreator.setDenomExclusions(resultSet.getString("DENO_EXCLUSIONS"));
				measureCreator.setNumerator(resultSet.getString("numerator"));
				measureCreator.setNumeratorExclusions(resultSet.getString("num_exclusion"));				
				measureCreator.setId(resultSet.getInt("measure_id")); 
				measureCreator.setMeasureCategory(resultSet.getString("CATEGORY_NAME"));
				measureCreator.setDescription(resultSet.getString("description"));
				measureCreator.setName(resultSet.getString("measure_name"));
				measureCreator.setProgramName(resultSet.getString("PROGRAM_NAME"));
				measureCreator.setSteward(resultSet.getString("steward_id"));			
				measureCreator.setTargetAge(resultSet.getString("target_population_age"));
				measureCreator.setType(typeMap.get(resultSet.getString("type_id")));
				measureCreator.setMeasureEditId(resultSet.getInt("MEASURE_EDIT_ID"));
				measureCreator.setStatus(resultSet.getString("STATUS_ID"));
				measureCreator.setIsActive(resultSet.getString("IS_ACTIVE"));
				measureCreator.setStartDate(resultSet.getString("START_DATE"));
				measureCreator.setEndDate(resultSet.getString("END_DATE"));				
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
			resultSet = statement.executeQuery("select * from qms_measure order by REC_UPDATE_DATE desc");
			int measureId = 0;
			while (resultSet.next()) {
				measureId = resultSet.getInt("measure_id");
				
				if(uniqueMeasureId.contains(measureId)) {
					continue;
				} else {
					uniqueMeasureId.add(measureId);	
					measureCreator = new MeasureCreator();					
					measureCreator.setId(measureId);
					measureCreator.setName(resultSet.getString("measure_name"));
					measureCreator.setProgramName(programMap.get(resultSet.getString("QUALITY_PROGRAM_ID")));
					measureCreator.setStatus(statusMap.get(resultSet.getString("status_id")));
					measureCreator.setReviewComments(resultSet.getString("review_comments"));
					measureCreator.setReviewedBy(userMap.get(resultSet.getString("REVIEWER_ID")));
					measureCreator.setIsActive(resultSet.getString("IS_ACTIVE"));
					measureCreator.setStartDate(resultSet.getString("START_DATE"));
					measureCreator.setEndDate(resultSet.getString("END_DATE"));					
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
	public RestResult updateMeasureWorkListStatus(int id, String status) {
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
			System.out.println(" Creating the MeasureCreator with version --> " + currentVersion + " for id --> " + lastCreatedCreator.getId());
			return insertMeasureCreator(lastCreatedCreator);			
		} else {
			System.out.println(" No need to update the status as current status and selected status are same for id --> " + id);
			RestResult restResult = new RestResult();
			restResult.setStatus(RestResult.FAIL_STATUS);
			restResult.setMessage("Current status and selected status is same. ");			
			return restResult;
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
			resultSet = statement.executeQuery("select QUALITY_PROGRAM_ID from QMS_QUALITY_PROGRAM where PROGRAM_NAME='"+programId+"' and CATEGORY_NAME='"+categoryName+"'");
			
			if (resultSet.next()) {
				qualityProgramId = resultSet.getString("QUALITY_PROGRAM_ID");				
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

}
