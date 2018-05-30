package com.qms.rest.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import java.util.Set;
import java.util.TreeSet;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.qms.rest.model.ColumnData;
import com.qms.rest.model.MeasureConfig;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.TableData;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSProperty;

@Service("measureConfigService")
//@PropertySource("classpath:application.properties")
public class MeasureConfigServiceImpl implements MeasureConfigService {
	
	@Autowired
	private QMSConnection qmsConnection;
	
	@Autowired
	private QMSProperty qmsProperty;	

	@Override
	public Set<TableData> getMeasureConfigData() {
		Set<String> tableNamesSet = getAllTableNames();
		Set<TableData> tableDataSet = new TreeSet<>();
		Connection connection = null;
		Statement stmt = null;
		ResultSet rs = null;		
		try {
			//connection = getConnection(schemaName, password);
			//connection = qmsConnection.getOracleConnection(schemaName, password);
			connection = qmsConnection.getOracleMeasureConfigConnection();
			stmt = connection.createStatement();
			TableData tableData = null;
			Set<ColumnData> columnsData = null;
			
			for (String tableName : tableNamesSet) {
				tableData = new TableData();
				tableData.setName(tableName);
				columnsData = new HashSet<>();
				tableData.setColumnList(columnsData);
				rs = stmt.executeQuery("SELECT column_name, data_type, data_length"
			            + " FROM user_tab_columns"
			            + " WHERE table_name = '"+tableName+"' order by column_name");
				ColumnData columnData = null;
			    while (rs.next()) {
			    	columnData = new ColumnData();				    	
			    	columnData.setName(rs.getString(1));
			    	columnData.setDataType(rs.getString(2));
			    	columnsData.add(columnData);
			    }	
			    tableDataSet.add(tableData);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
		finally {
			//QMSServiceImpl.closeJDBCResources(rs, stmt, connection);
			qmsConnection.closeJDBCResources(rs, stmt, connection);
		}		
		
		return tableDataSet;
	}
	
	private Set<String> getAllTableNames() {
		Set<String> dataSet = new TreeSet<>();
		Connection connection = null;
		DatabaseMetaData metadata;
		ResultSet tables = null;
		try {
			//connection = getConnection(schemaName, password);
			//connection = qmsConnection.getOracleConnection(schemaName, password);
			connection = qmsConnection.getOracleMeasureConfigConnection();			
			metadata = connection.getMetaData();
			String[] names = {"TABLE"}; 
			tables = metadata.getTables(null, null, "%", names);
			while (tables.next()) {  
				String tableSchema = tables.getString(2);
				//if (tableSchema.equalsIgnoreCase(schemaName)) {
				if (tableSchema.equalsIgnoreCase(qmsProperty.getOracleMeasureConfigUserName())) {
					dataSet.add(tables.getString(3));
				}				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
		finally {
			//QMSServiceImpl.closeJDBCResources(tables, null, connection);
			qmsConnection.closeJDBCResources(tables, null, connection);
		}		
		
		return dataSet;		
	}
	

	@Override
	public RestResult insertMeasureConfig(List<MeasureConfig> measureConfigList, String category) {
		
		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = null;
		ResultSet resultSet = null;
		Statement getStatement = null;		
		try {								
			//connection = QMSServiceImpl.getConnection();
			connection = qmsConnection.getOracleConnection();
			String measureId = measureConfigList.get(0).getMeasureId();
			int version = 0;
			getStatement = connection.createStatement();
			resultSet = getStatement.executeQuery("select max(version) from QMS_MEASURE_CONFIGURATOR where MEASURE_ID='"+measureId+"' and CATEGORY='"+category+"'");
			if(resultSet.next()) {
				version = resultSet.getInt(1);
			}
			version = version + 1;
			
			System.out.println(" Adding measure config for category " + category + " with version " + version + " for measure id " + measureId);
			
			String sqlStatementInsert = 
					"insert into QMS_MEASURE_CONFIGURATOR values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";			
			statement = connection.prepareStatement(sqlStatementInsert);
			
			int i=0;
			int lineId = 1;
			for (MeasureConfig measureConfig : measureConfigList) {		
				//Random ran = new Random();
				//int measureConfigId = ran.nextInt(50000);				
				//statement.setString(++i, measureConfigId+"");
				statement.setString(++i, measureConfig.getMeasureId());
				statement.setString(++i, category);
				statement.setInt(++i, lineId);
				lineId++;
				statement.setString(++i, measureConfig.getOperator());
				statement.setString(++i, measureConfig.getBusinessExpression());
				statement.setString(++i, measureConfig.getTechnicalExpression());
				statement.setString(++i, measureConfig.getRemarks());
				statement.setInt(++i, version);			
				statement.setString(++i, measureConfig.getStatus());
				statement.setString(++i, "Y");
				Date date = new Date();
				statement.setTimestamp(++i, new Timestamp(date.getTime()));			
				statement.setString(++i, "2");
				statement.addBatch();	

				i=0;
			}
			int [] ids = statement.executeBatch();
			
			restResult = RestResult.getRestResult(RestResult.SUCCESS_STATUS, "Create measure configuration.");
		} catch (Exception e) {
			e.printStackTrace();
			restResult = RestResult.getRestResult(RestResult.FAIL_STATUS, e.getMessage());
		}
		finally {
			//QMSServiceImpl.closeJDBCResources(null, getStatement, null);
			//QMSServiceImpl.closeJDBCResources(resultSet, statement, connection);
			qmsConnection.closeJDBCResources(null, getStatement, null);
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}	
		return restResult;
	}	
	
	@Override
	public List<MeasureConfig> getMeasureConfigById(String measureId, String category) {
		List<MeasureConfig> measureConfigList = new ArrayList<>();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			//connection = QMSServiceImpl.getConnection();
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();
			int latestVersion = 0;
			resultSet = statement.executeQuery("select max(version) from QMS_MEASURE_CONFIGURATOR where MEASURE_ID='"+measureId+"' and CATEGORY='"+category+"'");			
			if(resultSet.next()) {
				latestVersion = resultSet.getInt(1);
				if(latestVersion > 0) {	
					resultSet.close();
					resultSet = statement.executeQuery("select * from QMS_MEASURE_CONFIGURATOR where "
							+ "MEASURE_ID='"+measureId+"' and CATEGORY='"+category+"' and VERSION="+latestVersion);
					MeasureConfig measureConfig = null;
					while (resultSet.next()) {
						measureConfig = new MeasureConfig();
						measureConfig.setMeasureId(resultSet.getString("MEASURE_ID"));
						measureConfig.setCategory(resultSet.getString("CATEGORY"));
						measureConfig.setCategoryLineId(resultSet.getInt("CATEGORY_LINE_ID"));
						measureConfig.setOperator(resultSet.getString("OPERATOR"));
						measureConfig.setBusinessExpression(resultSet.getString("BUSINESS_EXPRESSION"));
						measureConfig.setTechnicalExpression(resultSet.getString("TECHNICAL_EXPRESSION"));
						measureConfig.setRemarks(resultSet.getString("REMARKS"));
						measureConfig.setVersion(resultSet.getInt("VERSION"));
						measureConfig.setStatus(resultSet.getString("STATUS"));
						measureConfig.setModifiedBy(resultSet.getString("MODIFIED_BY"));								
						measureConfigList.add(measureConfig);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			//QMSServiceImpl.closeJDBCResources(resultSet, statement, connection);
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			

		return measureConfigList;		
	}

	@Override
	public RestResult updateMeasureConfig(List<MeasureConfig> measureConfig, String category) {
		// TODO Auto-generated method stub
		return null;
	}	

}
