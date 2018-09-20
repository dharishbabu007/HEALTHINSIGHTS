package com.qms.rest.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.qms.rest.model.DimPatient;
import com.qms.rest.model.MemberDetail;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;

@Service("patientService")
public class PatientServiceImpl implements PatientService {
	
	@Autowired
	private QMSConnection qmsConnection;
	
	
	int cacheSize = 10;
	HashMap<String, DimPatient> cacheMap = new HashMap<>();
	Set<MemberDetail> memberDetailsSet = new HashSet<>();
	
	@PostConstruct
	public void cacheMemberData() {
		getMemberDetails();
		System.out.println(" SPV Caching member data for ........." + cacheSize);
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select member_id from HEDIS_MEMBER_WEB_VIEW limit "+cacheSize);
			Set<String> memberIds = new HashSet<String>();
			int i = 0;
			while (resultSet.next()) {
				memberIds.add(resultSet.getString("member_id"));
				if(i == cacheSize) break;
				i++;
			}
			
			DimPatient dimPatient = null;
			i = 0;
			System.out.println(" finished caches .. ");
			for (String memberId : memberIds) {
				if(connection == null || connection.isClosed()) {
					connection = qmsConnection.getHiveConnection();
					statement = connection.createStatement();
				}
				i++;	
				//System.out.println(" Caching member detail ... " + memberId);
				if(cacheMap.get(memberId) == null) {
					dimPatient = getMemberByIdFromDB(memberId, connection, statement);
					cacheMap.put(memberId, dimPatient);
				}				
				System.out.print(i+ " ");
				//System.out.println("  Cached member detail ... " + memberId);
			}
			System.out.println("  SPV Cached member detail completed *****");
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			System.out.println(" SPV actual cache size --> " + cacheMap.size());
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		
	}
	@PreDestroy
	public void cleanUp() throws Exception {}
	
	@Override
	public DimPatient getMemberById(String memberId) {
		Statement statement = null;
		Connection connection = null;		
		DimPatient dimPatient = cacheMap.get(memberId);
		
		if(dimPatient != null) {
			System.out.println(memberId + " Getting member id from cache - " + dimPatient.getEmailAddress());
		} else {
			System.out.println(memberId + " Getting member id from HIVE ");
			try {
				connection = qmsConnection.getHiveConnection();
				statement = connection.createStatement();
				dimPatient = getMemberByIdFromDB(memberId, connection, statement);
				cacheMap.put(memberId, dimPatient);
			} catch (Exception e) {
				e.printStackTrace();
			}
			finally {			
				qmsConnection.closeJDBCResources(null, statement, connection);
			}			
		}		
		return dimPatient;
	}
	
	
	
	
	
	public DimPatient getMemberByIdFromDB(String memberId, Connection connection, Statement statement) {
//		System.out.println(" Getting member detail from hive for " + memberId);
		DimPatient dimPatient = new DimPatient();
//		Statement statement = null;
		ResultSet resultSet = null;		
//		Connection connection = null;
		try {						
			//connection = QMSServiceImpl.getHiveConnection();
//			connection = qmsConnection.getHiveConnection();
//			statement = connection.createStatement();			
			
//			String memberSQL = "SELECT MEMBER_ID,EMAIL_ADDRESS,PHONE,ETHNICITY,GENDER,"+
//			"FIRST_NAME ||' '|| MIDDLE_NAME ||' '|| LAST_NAME AS \"Member_Name\","+
//			"ADDRESS1 ||', '|| ADDRESS2 ||', '|| CITY ||', '|| STATE ||', '|| ZIP AS \"Address\","+ 
//			"FLOOR(TRUNC(SYSDATE - (To_date(substr(ENC.DATE_OF_BIRTH_SK, 1, 4) || '-' || substr(ENC.DATE_OF_BIRTH_SK, 5,2) || '-' || substr(ENC.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')))/365.25) as \"Age\" "+ 
//			"FROM DIM_MEMBER ENC where MEMBER_ID='"+patientId+"'";
			
			String memberSQL = "SELECT MEMBER_ID,EMAIL_ADDRESS,PHONE,ETHNICITY,GENDER,"+
			"CONCAT(FIRST_NAME,' ',MIDDLE_NAME,' ',LAST_NAME) AS Name,"+
			"CONCAT(ADDRESS1,',',ADDRESS2,',',CITY,',',STATE,',',ZIP) AS Address,"+ 
			"cast(floor(datediff (CURRENT_DATE, (to_date(CONCAT(substr(ENC.DATE_OF_BIRTH_SK, 1, 4),'-',substr(ENC.DATE_OF_BIRTH_SK, 5,2),'-',substr(ENC.DATE_OF_BIRTH_SK, 7,2)))))/365.25) as float) Age "+ 
			"FROM DIM_MEMBER ENC WHERE MEMBER_ID='"+memberId+"'";			
			
			resultSet = statement.executeQuery(memberSQL);			
			
			while (resultSet.next()) {				
				dimPatient.setEmailAddress(resultSet.getString("email_address"));				
				dimPatient.setEthniCity(resultSet.getString("ethnicity"));
				dimPatient.setGender(resultSet.getString("gender")!=null?resultSet.getString("gender").trim():null);
				dimPatient.setPhone(resultSet.getString("PHONE"));
				dimPatient.setName(resultSet.getString("Name"));
				dimPatient.setAddress(resultSet.getString("Address"));
				dimPatient.setPatId(resultSet.getString("MEMBER_ID"));
				dimPatient.setAge(resultSet.getString("Age"));								
			}
			
			
//			resultSet.close();
//			memberSQL = "SELECT DM.MEMBER_ID, FC.CLAIMS_ID,"+ 
//			"MAX(TO_DATE(substr(FC.END_DATE_SK, 1, 4) || '-' || substr(FC.END_DATE_SK, 5,2) || '-' || substr(FC.END_DATE_SK, 7,2),'YYYY-MM-DD')) AS \"MAXDATE\" "+
//			"FROM FACT_CLAIMS FC "+
//			"INNER JOIN DIM_MEMBER DM ON DM.MEMBER_DIM_SK = FC.MEMBER_DIM_SK "+
//			"WHERE DM.MEMBER_ID = '"+patientId+"' " +
//			"GROUP BY DM.MEMBER_ID, FC.CLAIMS_ID";
//			resultSet = statement.executeQuery(memberSQL);
//			while (resultSet.next()) {
//				dimPatient.setLastDateService(resultSet.getString("MAXDATE"));
//			}
			
			////
			resultSet.close();
			memberSQL = "SELECT DM.MEMBER_ID, DP.PAYER_NAME "+
			"FROM FACT_MEMBERSHIP FM "+
			"INNER JOIN DIM_MEMBER DM ON DM.MEMBER_SK = FM.MEMBER_SK "+
			"INNER JOIN DIM_PAYER DP ON DP.PAYER_SK = FM.PRIMARY_PAYER_SK "+
			"WHERE DM.MEMBER_ID = '"+memberId+"'";			
			resultSet = statement.executeQuery(memberSQL);
			while (resultSet.next()) {
				dimPatient.setPrimaryPayer(resultSet.getString("PAYER_NAME"));
			}			
						
			resultSet.close();
			memberSQL = "SELECT DM.MEMBER_ID, cci_score AS MRA_Score "+
			"FROM fact_cci_risk_score FHS "+
			"INNER JOIN DIM_MEMBER DM ON DM.MEMBER_SK = FHS.MEMBER_SK "+
			"WHERE DM.MEMBER_ID = '"+memberId+"'";
			resultSet = statement.executeQuery(memberSQL);
			while (resultSet.next()) {				
				dimPatient.setMraScore(resultSet.getString("MRA_Score"));
				//int mraScore = Integer.parseInt(resultSet.getString("MRA_Score"));
				double mraScore = Double.parseDouble(resultSet.getString("MRA_Score"));
				//0-2 - low 2-4 medium 4-6 high > 6 catastrophic
				if(mraScore >= 6) 
					dimPatient.setRisk("catastrophic");
				else if (mraScore >= 4 && mraScore < 6) 
					dimPatient.setRisk("high");
				else if (mraScore >= 2 && mraScore < 4) 
					dimPatient.setRisk("medium");
				else if (mraScore >= 0 && mraScore < 2)
					dimPatient.setRisk("low");
			}
			

			//Comorbidities
			resultSet.close();
			memberSQL = "select fmc.* from fact_mem_comorbidity fmc, dim_member dm where "
					+ "fmc.member_sk = dm.member_sk and dm.member_id = '"+memberId+"'";
			resultSet = statement.executeQuery(memberSQL);
			Set<String> comorbidities = new TreeSet<>();
			ResultSetMetaData rsmd = resultSet.getMetaData();
			String colName = null;
			String colValue = null;
			while (resultSet.next()) {
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					colName = rsmd.getColumnName(i); 
					colValue = resultSet.getString(i);
					if(colValue != null && colValue.equalsIgnoreCase("1") && !colName.equalsIgnoreCase("fmc.comorbidity_count"))
						comorbidities.add(colName.replaceFirst("fmc.", ""));
                }
				break;
			}
			dimPatient.setComorbidities(comorbidities);
			dimPatient.setComorbiditiesCount(comorbidities.size()+"");
			
			//Care Gaps			
			resultSet.close();
			memberSQL = "select dqm.measure_title, qgl.status from dim_quality_measure dqm "
					+ "inner join qms_gic_lifecycle qgl on dqm.quality_measure_id = qgl.quality_measure_id "
					+ "where qgl.status <> 'closed' and qgl.member_id = '"+memberId+"'";
			Set<String> careGaps = new TreeSet<>();
			resultSet = statement.executeQuery(memberSQL);
			
			while (resultSet.next()) {
				careGaps.add(resultSet.getString("measure_title"));
			}
			dimPatient.setCareGaps(careGaps);
			dimPatient.setCareGapsCount(careGaps.size()+"");
			
			//PCP Name, NPI, NPI, Speciality, address 
			resultSet.close();
			memberSQL = "select dp.* from fact_mem_attribution fa, dim_provider dp, dim_member dm "
					+ "where dm.member_sk = fa.member_sk and fa.provider_sk = dp.provider_sk and dm.member_id='"+memberId+"'";
			resultSet = statement.executeQuery(memberSQL);
			while (resultSet.next()) {
				dimPatient.setProviderFirstName(resultSet.getString("first_name"));
				dimPatient.setProviderLastName(resultSet.getString("last_name"));
				dimPatient.setProviderBillingTaxId(resultSet.getString("billing_tax_id"));
				dimPatient.setProviderSpeciality(resultSet.getString("speciality_1"));
				dimPatient.setProviderAddress1(resultSet.getString("address1"));
				dimPatient.setProviderAddress2(resultSet.getString("address2"));
			}		
			
			if(true) {
				return dimPatient;
			}
			
			//TODO: change bill type to claim type 			
			resultSet.close();
			memberSQL = "SELECT DM.MEMBER_ID, "+
			"COUNT (CASE WHEN BILL_TYPE_DESC BETWEEN 111 AND 128 THEN 'IP' END) AS IP_VISIT, "+
			"COUNT (case WHEN BILL_TYPE_DESC BETWEEN 131 AND 138 THEN 'OP' END) As OP_VISIT, "+
			"COUNT (case WHEN BILL_TYPE_DESC BETWEEN 450 AND 459 THEN 'ER' END) AS ER_VISIT "+
			"FROM DIM_MEMBER DM "+
			"INNER JOIN FACT_CLAIMS FC ON FC.MEMBER_SK = DM.MEMBER_SK "+
			"INNER JOIN REF_BILL_TYPE RBY ON RBY.BILL_TYPE_ID = FC.BILL_TYPE_ID "+
			"WHERE DM.MEMBER_ID = '"+memberId+"' "+
			"GROUP BY DM.MEMBER_ID";			
			resultSet = statement.executeQuery(memberSQL);
			while (resultSet.next()) {
				dimPatient.setIpVisitsCount(resultSet.getString("IP_VISIT"));
				dimPatient.setOpVisitsCount(resultSet.getString("OP_VISIT"));
				dimPatient.setErVisitsCount(resultSet.getString("ER_VISIT"));
			}						
			
			
			resultSet.close();
			memberSQL = "SELECT DP.PAT_ID, FA.APPOINTMENT_DATE, "+ 
			"FROM FACT_APPOINTMENT FA "+
			"INNER JOIN DIM_PATIENT ON DP.PATIENT_SK = FA.PATIENT_SK "+
			"WHERE DP.PAT_ID = '"+memberId+"'";			
			resultSet = statement.executeQuery(memberSQL);
			while (resultSet.next()) {
				dimPatient.setNextAppointmentDate(resultSet.getString("APPOINTMENT_DATE"));
				//dimPatient.setPhysicianName(resultSet.getString("APPOINTMENT_DATE"));
				//dimPatient.setDepartment(resultSet.getString("APPOINTMENT_DATE"));
			}			
			
			//clinical world - getting the Procedures
			resultSet.close();
			memberSQL = "SELECT MAX(FP.ORDER_DATE), DP.PATIENT_ID "+ 
			"FROM DIM_PATIENT DP "+
			"INNER JOIN FACT_PROCEDURE FP ON FP.PATIENT_SK = DP.PATIENT_SK "+
			"INNER JOIN REF_PROCEDURES RP ON RP.PROCEDURE_ID = FP.PROCEDURE_ID "+
			"WHERE DM.MEMBER_ID = '"+memberId+"' "+
			"GROUP BY DP.PATIENT_ID";			
			resultSet = statement.executeQuery(memberSQL);
			while (resultSet.next()) {
				
			}
			
			//clinical world - getting Medical Prescription
			resultSet.close();
			memberSQL = "SELECT MAX(FM.ORDERING_DATE), DP.PATIENT_ID, M.MEDICATION_NAME "+ 
			"FROM DIM_PATIENT DP "+
			"INNER JOIN FACT_MEDICATION FM ON FM.PATIENT_SK = DP.PATIENT_SK "+
			"INNER JOIN MEDICATIONS M ON M.NDC_CODE = FM.NDC "+
			"WHERE DP.PAT_ID = '"+memberId+"' "+
			"GROUP BY DP.PATIENT_ID, M.MEDICATION_NAME";			
			resultSet = statement.executeQuery(memberSQL);
			while (resultSet.next()) {
				
			}			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
//		finally {			
//			qmsConnection.closeJDBCResources(resultSet, statement, connection);
//		}			
//		System.out.println(" Returned member detail from hive for " + memberId);
		return dimPatient;
	}
	
	@Override
	public DimPatient getPatientById(String patientId) {
		DimPatient dimPatient = new DimPatient();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			
			String memberSQL = "SELECT pat_id,EMAIL_ADDRESS,ETHNICITY,GENDER,pat_mrn,"+
			"CONCAT(PAT_FIRST_NAME,' ',PAT_MIDDLE_NAME,' ',PAT_LAST_NAME) AS Name,"+
			"CONCAT(ADD_LINE_1,',',ADD_LINE_2,',',PAT_CITY,',',PAT_STATE,',',ZIP) AS Address,"+
			"floor(datediff (CURRENT_DATE, (to_date(CONCAT(substr(ENC.DATE_OF_BIRTH_SK, 1, 4),'-',substr(ENC.DATE_OF_BIRTH_SK, 5,2),'-',substr(ENC.DATE_OF_BIRTH_SK, 7,2)))))/365.25) Age "+ 
			"FROM DIM_PATIENT ENC WHERE pat_mrn='"+patientId+"'";			
			
			resultSet = statement.executeQuery(memberSQL);						
			while (resultSet.next()) {
				dimPatient.setEmailAddress(resultSet.getString("email_address"));				
				dimPatient.setEthniCity(resultSet.getString("ethnicity"));
				dimPatient.setGender(resultSet.getString("gender")!=null?resultSet.getString("gender").trim():null);				
				dimPatient.setName(resultSet.getString("Name"));
				dimPatient.setAddress(resultSet.getString("Address"));
				dimPatient.setPatId(resultSet.getString("pat_id"));
				dimPatient.setMrn(resultSet.getString("pat_mrn"));
				dimPatient.setAge(resultSet.getString("Age"));								
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {			
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			

		return dimPatient;			
	}

	@Override
	public User getUserInfo(String userName, String password) {
		
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		User user = null;
		try {						
			connection = qmsConnection.getOracleConnection();
			statement = connection.createStatement();			
			resultSet = statement.executeQuery("select * from QMS_USER_MASTER where USER_LOGINID='"+userName+"' and PASSWORD='"+password+"'");
			while (resultSet.next()) {
				user = new User();
				user.setEmail(resultSet.getString("USER_EMAIL"));
				user.setId(resultSet.getString("USER_ID"));
				user.setLoginId(resultSet.getString("USER_LOGINID"));
				user.setName(resultSet.getString("USER_NAME"));
				user.setRoleId(resultSet.getString("USER_ROLE_ID"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		
		return user;
	}

	@Override
	public Set<MemberDetail> getMemberDetails() {
		if(memberDetailsSet.size() > 0) {
			return memberDetailsSet;
		}
		
		System.out.println(" Getting member details from HIVE... ");
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
//			resultSet = statement.executeQuery("select hmv.* from hedis_member_view hmv "+
//						"inner join HEDIS_SUMMARY_VIEW hsv on hmv.QUALITY_MEASURE_SK = hsv.quality_measure_sk");
			resultSet = statement.executeQuery("select * from HEDIS_MEMBER_WEB_VIEW limit " + cacheSize);

			MemberDetail data = null;
			while (resultSet.next()) {
				data = new MemberDetail(); 
				data.setId(resultSet.getString("member_id"));
				data.setAge(resultSet.getString("age"));
				data.setAmount(resultSet.getString("amount_paid").equalsIgnoreCase("0")?"0":"$"+resultSet.getString("amount_paid"));
				data.setGender(resultSet.getString("gender").equalsIgnoreCase("F")?"Female":"Male");
				data.setHccScore(resultSet.getString("cci_score"));
				data.setName(resultSet.getString("name"));
				data.setReason(resultSet.getString("reason"));				
				memberDetailsSet.add(data);				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}		
		System.out.println(" Member details from Hive returned " + memberDetailsSet.size());
		return memberDetailsSet;	
	}

}
