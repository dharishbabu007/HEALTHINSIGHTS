package com.qms.rest.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.qms.rest.model.DimPatient;
import com.qms.rest.model.User;
import com.qms.rest.util.QMSConnection;

@Service("patientService")
public class PatientServiceImpl implements PatientService {
	
	@Autowired
	private QMSConnection qmsConnection;
	
	@Override
	public DimPatient getMemberById(String memberId) {
		DimPatient dimPatient = new DimPatient();
		Statement statement = null;
		ResultSet resultSet = null;		
		Connection connection = null;
		try {						
			//connection = QMSServiceImpl.getHiveConnection();
			connection = qmsConnection.getHiveConnection();
			statement = connection.createStatement();			
			
//			String memberSQL = "SELECT MEMBER_ID,EMAIL_ADDRESS,PHONE,ETHNICITY,GENDER,"+
//			"FIRST_NAME ||' '|| MIDDLE_NAME ||' '|| LAST_NAME AS \"Member_Name\","+
//			"ADDRESS1 ||', '|| ADDRESS2 ||', '|| CITY ||', '|| STATE ||', '|| ZIP AS \"Address\","+ 
//			"FLOOR(TRUNC(SYSDATE - (To_date(substr(ENC.DATE_OF_BIRTH_SK, 1, 4) || '-' || substr(ENC.DATE_OF_BIRTH_SK, 5,2) || '-' || substr(ENC.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')))/365.25) as \"Age\" "+ 
//			"FROM DIM_MEMBER ENC where MEMBER_ID='"+patientId+"'";
			
			String memberSQL = "SELECT MEMBER_ID,EMAIL_ADDRESS,PHONE,ETHNICITY,GENDER,"+
			"CONCAT(FIRST_NAME,' ',MIDDLE_NAME,' ',LAST_NAME) AS Name,"+
			"CONCAT(ADDRESS1,',',ADDRESS2,',',CITY,',',STATE,',',ZIP) AS Address,"+ 
			"floor(datediff (CURRENT_DATE, (to_date(CONCAT(substr(ENC.DATE_OF_BIRTH_SK, 1, 4),'-',substr(ENC.DATE_OF_BIRTH_SK, 5,2),'-',substr(ENC.DATE_OF_BIRTH_SK, 7,2)))))/365.25) Age "+ 
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
			memberSQL = "SELECT DM.MEMBER_ID, HCC_SCORE AS MRA_Score "+
			"FROM FACT_HCC_RISK_SCORE FHS "+
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
			
			if(true) {
				return dimPatient;
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
		finally {			
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}			

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
			//connection = QMSServiceImpl.getConnection();
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
			//QMSServiceImpl.closeJDBCResources(resultSet, statement, connection);
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		
		return user;
	}

}
