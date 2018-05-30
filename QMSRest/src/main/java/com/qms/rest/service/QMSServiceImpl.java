package com.qms.rest.service;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.Measure;
import com.qms.rest.model.MeasureCreator;
import com.qms.rest.model.NameValue;
import com.qms.rest.model.RestResult;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSProperty;


@Service("qmsService")
public class QMSServiceImpl implements QMSService {
	
//	public static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver"; //org.apache.hadoop.hive.jdbc.HiveDriver
//	public static final String HIVE_JDBC_EMBEDDED_CONNECTION = "jdbc:hive2://192.168.1.7:10000/default";
//
//	public static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.OracleDriver"; 
//	public static final String ORACLE_JDBC_EMBEDDED_CONNECTION = "jdbc:oracle:thin:@wslave2:1521:healthin";
//	//public static final String ORACLE_JDBC_EMBEDDED_CONNECTION = "jdbc:oracle:thin:@BLRTRIFWN27368:1521:XE"; //abhinav
//	//public static final String ORACLE_JDBC_EMBEDDED_CONNECTION = "jdbc:oracle:thin:@BLRTRICWN25268:1521:healthin"; //nitin

	@Autowired
	private QMSConnection qmsConnection;	
	
	@Override
	public Set<MeasureCreator> getMeasureLibrary(String programName, String value) {		
		Set<MeasureCreator> measureList = new LinkedHashSet<>();
		HashMap<String, String> programMap = getIdNameMap("QMS_QUALITY_PROGRAM", "QUALITY_PROGRAM_ID", "PROGRAM_NAME");
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		HashMap<String, String> stewardMap = getIdNameMap("QMS_MEASURE_STEWARD", "STEWARD_TYPE_ID", "STEWARD_NAME");		
		
		String whereClause = "where STATUS_ID='5'";
		if(programName.equalsIgnoreCase("Reimbursement")) {
			//String programId = programMap.get(value);
			//whereClause = " where STATUS_ID='5' and QUALITY_PROGRAM_ID='"+programId+"'";
			whereClause = " where STATUS_ID='5' and QUALITY_PROGRAM_ID in (select QUALITY_PROGRAM_ID from QMS_QUALITY_PROGRAM where PROGRAM_NAME='"+value+"')";
		}
		else if(programName.equalsIgnoreCase("Clinical")) {
			whereClause = " where STATUS_ID='5' and clinical_conditions='"+value+"'";													
		}
		else if(programName.equalsIgnoreCase("NQF")) {
			whereClause = " where STATUS_ID='5' and domain='"+value+"'";
		}		
		
		System.out.println("****WhereClause --> " + whereClause);
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			//connection = getConnection();
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from QMS_MEASURE "+whereClause+" order by MEASURE_ID asc");			
			MeasureCreator measureCreator = null;
			while (resultSet.next()) {
				measureCreator = new MeasureCreator();
				measureCreator.setName(resultSet.getString("measure_name"));
				measureCreator.setClinocalCondition(resultSet.getString("clinical_conditions"));
				measureCreator.setId(resultSet.getString("measure_id"));
				measureCreator.setProgramName(programMap.get(resultSet.getString("QUALITY_PROGRAM_ID")));
				measureCreator.setSteward(stewardMap.get(resultSet.getString("STEWARD_ID")));
				measureCreator.setType(typeMap.get(resultSet.getString("TYPE_ID")));
				measureList.add(measureCreator);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			//closeJDBCResources(resultSet, statement, connection);
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}	
		
		return measureList;
	}

	
	@Override
	public Measure getMeasureLibraryById(int id) {
		Measure measure = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			//connection = getConnection();
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from qms_dim_quality_measure_main where quality_measure_id="+id);
			
			while (resultSet.next()) {
				measure = new Measure();
				measure.setName(resultSet.getString("measure_title"));
				measure.setClinocalCondition(resultSet.getString("clinical_conditions"));
				measure.setId(resultSet.getInt("quality_measure_id"));
				measure.setProgramName(resultSet.getString("program_name"));
				measure.setSteward(resultSet.getString("steward"));
				measure.setType(resultSet.getString("type"));				
				measure.setTargetAge(resultSet.getString("target_population_age"));
				measure.setMeasureDomain(resultSet.getString("nqs_domain"));
				measure.setMeasureCategory(resultSet.getString("category"));
				measure.setTarget(resultSet.getString("target"));
				measure.setDescription(resultSet.getString("description"));
				measure.setDenominator(resultSet.getString("denominator"));
				measure.setDenomExclusions(resultSet.getString("exclusions"));
				measure.setNumerator(resultSet.getString("numerator"));				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			//closeJDBCResources(resultSet, statement, connection);
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			

		return measure;
	}	
	
	
//	public static void closeJDBCResources (ResultSet resultSet, Statement statement, Connection connection) {
//		try {
//			if(resultSet != null) resultSet.close();
//			if(statement != null) statement.close();
//			if(connection != null) connection.close();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}		
//	}
	
//	public static Connection getConnection() throws Exception {
//		//HIVE
//		//Class.forName(HIVE_JDBC_DRIVER);
//		//Connection connection = DriverManager.getConnection(HIVE_JDBC_EMBEDDED_CONNECTION, "hive", "login@123");
//		
//		//ORACLE
//		Class.forName(ORACLE_JDBC_DRIVER);
//		Connection connection = DriverManager.getConnection(ORACLE_JDBC_EMBEDDED_CONNECTION, "admin", "admin123");		
//		
//		return connection;
//	}
//	
//	public static Connection getHiveConnection() throws Exception {
//		//HIVE
//		Class.forName(HIVE_JDBC_DRIVER);
//		Connection connection = DriverManager.getConnection(HIVE_JDBC_EMBEDDED_CONNECTION, "hive", "login@123");		
//		return connection;
//	}	
	
	
	@Override
	public Set<String> getQMSHomeDropDownList(String tableName, String columnName) {
		Set<String> dataSet = new HashSet<>();
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			//connection = getConnection();
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
			//closeJDBCResources(resultSet, statement, connection);
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
			//connection = getConnection();
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
			//closeJDBCResources(resultSet, statement, connection);
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
			//connection = getConnection();
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
			//closeJDBCResources(resultSet, statement, connection);
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		return dataSet;				
	}

	@Override
	public RestResult insertMeasureCreator(MeasureCreator measureCreator) {		
		HashMap<String, String> statusMap = getIdNameMap("QMS_MEASURE_STATUS", "MEASURE_STATUS_ID", "MEASURE_STATUS_NAME");
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		
		String qualityProgramId = this.getQualityProgramId(measureCreator.getProgramName(), measureCreator.getMeasureCategory());
		
		String sqlStatementInsert = 
				"insert into qms_measure (clinical_conditions,data_sources_id,denominator,target,domain,deno_exclusions,"
				+ "numerator,num_exclusion,measure_id,description,measure_name,QUALITY_PROGRAM_ID,"
				+ "target_population_age,type_id,measure_edit_id,REC_UPDATE_DATE,STATUS_ID,REVIEWER_ID,AUTHOR_ID,MODIFIED_BY) "
				+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = new RestResult();
		try {					
			int i=0;
			//connection = getConnection();
			connection = qmsConnection.getOracleConnection();
			statement = connection.prepareStatement(sqlStatementInsert);
			statement.setString(++i, measureCreator.getClinocalCondition());
			statement.setString(++i, measureCreator.getDataSource());
			statement.setString(++i, measureCreator.getDenominator());
			statement.setString(++i, measureCreator.getTarget()); 
			statement.setString(++i, measureCreator.getMeasureDomain());	 				
			statement.setString(++i, measureCreator.getDenomExclusions());
			
			statement.setString(++i, measureCreator.getNumerator());
			statement.setString(++i, measureCreator.getNumeratorExclusions());
			
			//first version
			System.out.println(" measureCreator.getMeasureEditId() --> " + measureCreator.getMeasureEditId());
			if(measureCreator.getMeasureEditId() == null) {
				Random ran = new Random();
				int measureId = 1000+ran.nextInt(5000);
				System.out.println(" Random measure Id --> "+measureId);
				statement.setString(++i, measureId+"");
				measureCreator.setId(measureId+"");
			} else {
				statement.setString(++i, measureCreator.getId());
			}
			
			//statement.setString(++i, measureCreator.getId());			
			//statement.setString(++i, measureCreator.getMeasureCategory());
			statement.setString(++i, measureCreator.getDescription());
			statement.setString(++i, measureCreator.getName());
			statement.setString(++i, qualityProgramId);
			
			//statement.setString(++i, measureCreator.getSteward());		
			statement.setString(++i, measureCreator.getTargetAge());
			statement.setString(++i, typeMap.get(measureCreator.getType()));	
			
			if(measureCreator.getMeasureEditId() == null)
				statement.setString(++i, "1"); //version
			else 
				statement.setString(++i, measureCreator.getMeasureEditId()); //version
			
			Date date = new Date();
			statement.setTimestamp(++i, new Timestamp(date.getTime()));			
			statement.setString(++i, statusMap.get(measureCreator.getStatus()==null?"In-Progress":measureCreator.getStatus()));
			statement.setString(++i, "2");
			statement.setString(++i, "1");
			statement.setString(++i, "1");
			statement.executeUpdate();
			System.out.println("Added the measure worklist with id --> " + measureCreator.getId() + " status --> " + 
					measureCreator.getStatus() + " version --> " + measureCreator.getMeasureEditId());
			restResult.setStatus(RestResult.SUCCESS_STATUS);
			restResult.setMessage(" New measure creation. ");
		} catch (Exception e) {
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
			//closeJDBCResources(null, statement, connection);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}	
		return restResult;
	}

	@Override
	public RestResult updateMeasureCreator(MeasureCreator measureCreator) {		
		HashMap<String, String> statusMap = getIdNameMap("QMS_MEASURE_STATUS", "MEASURE_STATUS_ID", "MEASURE_STATUS_NAME");
		
		//new version create
		MeasureCreator lastCreatedCreator = findMeasureCreatorById(measureCreator.getId());
		String lastStatusId = lastCreatedCreator.getStatus();
		String currentStatusId = statusMap.get(measureCreator.getStatus()==null?"In-Progress":measureCreator.getStatus());
		System.out.println(" lastStatusId --> " + lastStatusId + " currentStatusId --> " + currentStatusId);
		if(!lastStatusId.equalsIgnoreCase(currentStatusId)) {
			int currentVersion = Integer.parseInt(lastCreatedCreator.getMeasureEditId())+1;
			measureCreator.setMeasureEditId(currentVersion+"");
			System.out.println(" Creating the MeasureCreator with version --> " + currentVersion + " for id --> " + measureCreator.getId());
			return this.insertMeasureCreator(measureCreator);
		}
		
		String sqlStatementUpdate = 
				"update qms_measure set clinical_conditions=?, data_sources_id=?, denominator=?, target=?, "
				+ "domain=?, deno_exclusions=?, numerator=?, num_exclusion=?, description=?, measure_name=?, "
				+ "QUALITY_PROGRAM_ID=?, STEWARD_ID=?, target_population_age=?, type_id=?, rec_update_date=?,  STATUS_ID=? "
				+ "where measure_id=?";
		
		String qualityProgramId = this.getQualityProgramId(measureCreator.getProgramName(), measureCreator.getMeasureCategory());
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		
		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = new RestResult();
		try {					
			int i=0;
			//connection = getConnection();
			connection = qmsConnection.getOracleConnection();
			statement = connection.prepareStatement(sqlStatementUpdate);
			statement.setString(++i, measureCreator.getClinocalCondition());
			statement.setString(++i, measureCreator.getDataSource());
			statement.setString(++i, measureCreator.getDenominator());
			statement.setString(++i, measureCreator.getTarget()); 
			
			statement.setString(++i, measureCreator.getMeasureDomain());	 				
			statement.setString(++i, measureCreator.getDenomExclusions());
			statement.setString(++i, measureCreator.getNumerator());
			statement.setString(++i, measureCreator.getNumeratorExclusions());	
			//statement.setString(++i, measureCreator.getMeasureCategory());
			statement.setString(++i, measureCreator.getDescription());
			statement.setString(++i, measureCreator.getName());
			
			statement.setString(++i, qualityProgramId);
			statement.setString(++i, measureCreator.getSteward());			
			statement.setString(++i, measureCreator.getTargetAge());
			statement.setString(++i, typeMap.get(measureCreator.getType()));			
			Date date = new Date();
			statement.setTimestamp(++i, new Timestamp(date.getTime()));
			statement.setString(++i, statusMap.get(measureCreator.getStatus()==null?"In-Progress":measureCreator.getStatus()));
			
			statement.setString(++i, measureCreator.getId());
			
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
			//closeJDBCResources(null, statement, connection);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}
		return restResult;
	}

	@Override
	public MeasureCreator findMeasureCreatorById(String id) {
		HashMap<String, String> typeMap = getIdNameMap("QMS_MEASURE_TYPE", "MEASURE_TYPE_ID", "MEASURE_TYPE_NAME");
		MeasureCreator measureCreator = null;
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			//connection = getConnection();
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			//resultSet = statement.executeQuery("select * from qms_measure_worklist where measure_id='"+id+"'");
			resultSet = statement.executeQuery("select qm.*,qqp.PROGRAM_NAME,qqp.CATEGORY_NAME from qms_measure qm, QMS_QUALITY_PROGRAM qqp where qm.measure_id='"+id+"' and qm.QUALITY_PROGRAM_ID=qqp.QUALITY_PROGRAM_ID order by qm.REC_UPDATE_DATE desc");			
			
			while (resultSet.next()) {
				measureCreator = new MeasureCreator();
				measureCreator.setClinocalCondition(resultSet.getString("clinical_conditions"));
				measureCreator.setDataSource(resultSet.getString("data_sources_id"));
				measureCreator.setDenominator(resultSet.getString("denominator"));
				measureCreator.setTarget(resultSet.getString("target")); 
				measureCreator.setMeasureDomain(resultSet.getString("domain"));	 				
				measureCreator.setDenomExclusions(resultSet.getString("DENO_EXCLUSIONS"));
				measureCreator.setNumerator(resultSet.getString("numerator"));
				measureCreator.setNumeratorExclusions(resultSet.getString("num_exclusion"));				
				measureCreator.setId(resultSet.getString("measure_id")); 
				measureCreator.setMeasureCategory(resultSet.getString("CATEGORY_NAME"));
				measureCreator.setDescription(resultSet.getString("description"));
				measureCreator.setName(resultSet.getString("measure_name"));
				measureCreator.setProgramName(resultSet.getString("PROGRAM_NAME"));
				measureCreator.setSteward(resultSet.getString("steward_id"));			
				measureCreator.setTargetAge(resultSet.getString("target_population_age"));
				measureCreator.setType(typeMap.get(resultSet.getString("type_id")));
				measureCreator.setMeasureEditId(resultSet.getString("MEASURE_EDIT_ID"));
				measureCreator.setStatus(resultSet.getString("STATUS_ID"));
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			//closeJDBCResources(resultSet, statement, connection);
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
		List<String> uniqueMeasureId = new ArrayList<>();  
		try {						
			//connection = getConnection();
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from qms_measure order by REC_UPDATE_DATE desc");
			String measureId = null;
			while (resultSet.next()) {
				measureId = resultSet.getString("measure_id");
				
				if(uniqueMeasureId.contains(measureId)) {
//					System.out.println(" SCIPPED measure with id : " + measureId + " and version : " + resultSet.getString("MEASURE_EDIT_ID"));
					continue;
				} else {
//					System.out.println(" Added measure with id : " + measureId + " and version : " + resultSet.getString("MEASURE_EDIT_ID"));
					uniqueMeasureId.add(measureId);	
					measureCreator = new MeasureCreator();					
//					measureCreator.setClinocalCondition(resultSet.getString("clinical_conditions"));
//					measureCreator.setDataSource(resultSet.getString("data_sources"));
//					measureCreator.setDenominator(resultSet.getString("denominator"));
//					measureCreator.setTarget(resultSet.getString("target")); 
//					measureCreator.setMeasureDomain(resultSet.getString("domain"));	 				
//					measureCreator.setDenomExclusions(resultSet.getString("deno_exclusion"));
//					measureCreator.setNumerator(resultSet.getString("numerator"));
//					measureCreator.setNumeratorExclusions(resultSet.getString("num_exclusion"));				
//					measureCreator.setMeasureCategory(resultSet.getString("category"));
//					measureCreator.setDescription(resultSet.getString("description"));
//					measureCreator.setSteward(resultSet.getString("steward"));			
//					measureCreator.setTargetAge(resultSet.getString("target_population_age"));
//					measureCreator.setType(resultSet.getString("type"));
//					measureCreator.setMeasureEditId(resultSet.getString("MEASURE_EDIT_ID"));
					
					measureCreator.setId(measureId);
					measureCreator.setName(resultSet.getString("measure_name"));
					measureCreator.setProgramName(programMap.get(resultSet.getString("QUALITY_PROGRAM_ID")));
					measureCreator.setStatus(statusMap.get(resultSet.getString("status_id")));
					measureCreator.setReviewComments(resultSet.getString("review_comments"));
					measureCreator.setReviewedBy(userMap.get(resultSet.getString("REVIEWER_ID")));
					dataSet.add(measureCreator);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			//closeJDBCResources(resultSet, statement, connection);
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
			//connection = getConnection();
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
			//closeJDBCResources(resultSet, statement, connection);
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
		return statusMap;				
	}
	
	

	@Override
	public RestResult updateMeasureWorkListStatus(String id, String status) {
		System.out.println("SERVICE Update Measure WorkList Status for id : " + id + " with status : " + status);		
		HashMap<String, String> statusMap = getIdNameMap("QMS_MEASURE_STATUS", "MEASURE_STATUS_ID", "MEASURE_STATUS_NAME");
		
		//new version create
		MeasureCreator lastCreatedCreator = findMeasureCreatorById(id);
		String lastStatusId = lastCreatedCreator.getStatus();
		String currentStatusId = statusMap.get(status);
		System.out.println(" lastStatusId --> " + lastStatusId + " currentStatusId --> " + currentStatusId);
		if(!lastStatusId.equalsIgnoreCase(currentStatusId)) {
			int currentVersion = Integer.parseInt(lastCreatedCreator.getMeasureEditId())+1;
			lastCreatedCreator.setMeasureEditId(currentVersion+"");
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
			//connection = getConnection();
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
			//closeJDBCResources(resultSet, statement, connection);
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			
		System.out.println(" getQualityProgramId " + programId + " categoryName " + categoryName + " qualityProgramId " + qualityProgramId);
		return qualityProgramId;
	}	

}
