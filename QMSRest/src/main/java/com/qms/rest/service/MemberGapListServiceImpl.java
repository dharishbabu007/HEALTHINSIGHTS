package com.qms.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.model.QmsGicLifecycle;
import com.qms.rest.exception.QmsInsertionException;
import com.qms.rest.model.DimMemberGapListSearch;
import com.qms.rest.model.DimMemeberList;
import com.qms.rest.model.DimPatient;
import com.qms.rest.model.FactHedisGapsInCare;
import com.qms.rest.model.MemberCareGaps;
import com.qms.rest.model.MemberCareGapsList;
import com.qms.rest.model.QMSMemberReq;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSProperty;

@Service("memberGapList")
public class MemberGapListServiceImpl implements MemberGapListService {

	@Autowired
	private QMSConnection qmsConnection;

	@Autowired
	private QMSProperty qmsProperty;

	private Set<QmsGicLifecycle> dimMemberGaps;

	@Override
	public DimMemeberList findMembergapListByMid(String mid) {
		DimMemeberList dimMemeberList = new DimMemeberList();
		Map<String, QmsGicLifecycle> qmsGicLifyCycleMap = new HashMap<String, QmsGicLifecycle>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		System.out.println("Service inside" + mid);
		try {

			connection = qmsConnection.getHiveConnection();
			System.out.println("Service after connection " + connection);
			statement = connection.createStatement();

			/*String membergplistQry = "SELECT DM.MEMBER_SK, DM.MEMBER_ID, CONCAT(DM.FIRST_NAME,' ',DM.LAST_NAME)as name, DM.GENDER, DD.DATE_SK, DD.CALENDAR_DATE AS DATE_OF_BIRTH, QGL.MEMBER_ID, QGL.INTERVENTIONS, QGL.PRIORITY, QGL.PAYOR_COMMENTS, QGL.PROVIDER_COMMENTS, QGL.STATUS, FHGIC.MEMBER_SK, FHGIC.HEDIS_GAPS_IN_CARE_SK \r\n"
					+ "FROM DIM_MEMBER DM \r\n"
					+ "INNER JOIN FACT_HEDIS_GAPS_IN_CARE FHGIC ON FHGIC.MEMBER_SK = DM.MEMBER_SK \r\n"
					+ "INNER JOIN QMS_GIC_LIFECYCLE QGL ON QGL.MEMBER_ID = DM.MEMBER_ID \r\n"
					+ "INNER JOIN DIM_DATE DD ON DD.DATE_SK = DM.DATE_OF_BIRTH_SK where DM.MEMBER_ID=" + mid;*/
			String membergplistQry = "SELECT DM.MEMBER_ID, CONCAT(DM.FIRST_NAME,' ',DM.LAST_NAME) as name, DM.GENDER, DD.DATE_SK, DD.CALENDAR_DATE AS DATE_OF_BIRTH, QGL.MEMBER_ID, QGL.QUALITY_MEASURE_ID, QGL.INTERVENTIONS, QGL.PRIORITY, QGL.PAYOR_COMMENTS, QGL.PROVIDER_COMMENTS, QGL.STATUS, QM.QUALITY_MEASURE_ID,QM.MEASURE_TITLE  \r\n" + 
					"FROM DIM_MEMBER DM \r\n" + 
					"INNER JOIN QMS_GIC_LIFECYCLE QGL ON QGL.MEMBER_ID = DM.MEMBER_ID  \r\n" + 
					"INNER JOIN DIM_QUALITY_MEASURE QM ON  QGL.QUALITY_MEASURE_ID=QM.QUALITY_MEASURE_ID \r\n" + 
					"INNER JOIN DIM_DATE DD ON DD.DATE_SK = DM.DATE_OF_BIRTH_SK where DM.MEMBER_ID="+mid;

			resultSet = statement.executeQuery(membergplistQry);
			System.out.println("Service resultset" + resultSet.getFetchSize());
			if (resultSet.next()) {
				dimMemeberList.setMemberId(resultSet.getString("MEMBER_ID"));
//				dimMemeberList.setMemberSk(resultSet.getString("MEMBER_SK"));
				dimMemeberList.setName(resultSet.getString("name"));
				dimMemeberList
						.setGender(resultSet.getString("gender") != null ? resultSet.getString("gender").trim() : null);
				// dimMemeberList.setRecCreateDate(resultSet.getString("DATE_SK"));
				dimMemeberList.setDateOfBirthSk(resultSet.getString("DATE_OF_BIRTH"));
				// dimMemeberList.setRecCreateDate(resultSet.getDate("DATE_SK"));
				dimMemeberList.setQmsGicLifecycle(new ArrayList<QmsGicLifecycle>());

				QmsGicLifecycle qmsGicLifecycle = new QmsGicLifecycle();
				qmsGicLifecycle.setInterventions(resultSet.getString("INTERVENTIONS"));
				qmsGicLifecycle.setPriority(resultSet.getString("PRIORITY"));
				qmsGicLifecycle.setPayorComments(resultSet.getString("PAYOR_COMMENTS"));
				qmsGicLifecycle.setStatus(resultSet.getString("STATUS"));
				qmsGicLifecycle.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
				qmsGicLifyCycleMap.put(resultSet.getString("QUALITY_MEASURE_ID"), qmsGicLifecycle);
				
				FactHedisGapsInCare factHedisGapsInCare = new FactHedisGapsInCare();
				factHedisGapsInCare.setMeasureTitle(resultSet.getString("MEASURE_TITLE"));
				factHedisGapsInCare.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
				qmsGicLifecycle.setFactHedisGapsInCare(new ArrayList<FactHedisGapsInCare>());
				qmsGicLifecycle.getFactHedisGapsInCare().add(factHedisGapsInCare);

			}

			while (resultSet.next()) {
				System.out.println("inside ");
				String qmsMesureId = resultSet.getString("QUALITY_MEASURE_ID");
				if(qmsGicLifyCycleMap.containsKey(qmsMesureId)) {
					FactHedisGapsInCare factHedisGapsInCare = new FactHedisGapsInCare();
					factHedisGapsInCare.setMeasureTitle(resultSet.getString("MEASURE_TITLE"));
					factHedisGapsInCare.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
					qmsGicLifyCycleMap.get(qmsMesureId).getFactHedisGapsInCare().add(factHedisGapsInCare);
				}else {
					QmsGicLifecycle qmsGicLifecycle = new QmsGicLifecycle();
					qmsGicLifecycle.setInterventions(resultSet.getString("INTERVENTIONS"));
					qmsGicLifecycle.setPriority(resultSet.getString("PRIORITY"));
					qmsGicLifecycle.setPayorComments(resultSet.getString("PAYOR_COMMENTS"));
					qmsGicLifecycle.setStatus(resultSet.getString("STATUS"));
					qmsGicLifecycle.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
					qmsGicLifyCycleMap.put(resultSet.getString("QUALITY_MEASURE_ID"), qmsGicLifecycle);
					
					FactHedisGapsInCare factHedisGapsInCare = new FactHedisGapsInCare();
					factHedisGapsInCare.setMeasureTitle(resultSet.getString("MEASURE_TITLE"));
					factHedisGapsInCare.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
					qmsGicLifecycle.setFactHedisGapsInCare(new ArrayList<FactHedisGapsInCare>());
					qmsGicLifecycle.getFactHedisGapsInCare().add(factHedisGapsInCare);
				}

			}
			
			if(qmsGicLifyCycleMap.size() > 0) {
				dimMemeberList.getQmsGicLifecycle().addAll(qmsGicLifyCycleMap.values());
			}

			System.out.println("dim : " + dimMemeberList);
			System.out.println(" QMS : "
					+ dimMemeberList.getQmsGicLifecycle().size());
	
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}

		return dimMemeberList;
	}

	@Override
	public QMSMemberReq editMembergapListByQMS(QMSMemberReq qMSMemberReq) throws QmsInsertionException {
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;

		try {
			connection = qmsConnection.getHiveConnection();
			System.out.println("Service after connection " + connection);
			statement = connection.createStatement();
			
				PreparedStatement qmsRecord = connection.prepareStatement
					    ("INSERT INTO QMS_GIC_LIFECYCLE VALUES (\r\n" + 
					    		"(SELECT MAX(GIC_LIFECYCLE_ID)+1 FROM QMS_GIC_LIFECYCLE),\r\n" + 
					    		"(SELECT HEDIS_GAPS_IN_CARE_SK FROM QMS_GIC_LIFECYCLE WHERE QUALITY_MEASURE_ID = ? AND MEMBER_ID = ?),?,\r\n" + 
					    		"(SELECT PRODUCT_PLAN_ID FROM QMS_GIC_LIFECYCLE WHERE QUALITY_MEASURE_ID = ? AND MEMBER_ID = ?),?,\r\n" + 
					    		"(SELECT DATE FROM QMS_GIC_LIFECYCLE WHERE QUALITY_MEASURE_ID = ? AND MEMBER_ID = ?),?,?,?,?,?,\r\n" + 
					    		"(SELECT FILE_PATH FROM QMS_GIC_LIFECYCLE WHERE QUALITY_MEASURE_ID = ? AND MEMBER_ID = ?),\r\n" + 
					    		"(SELECT SERVICE_DATE_FROM FROM QMS_GIC_LIFECYCLE WHERE QUALITY_MEASURE_ID = ? AND MEMBER_ID = ?),\r\n" + 
					    		"(SELECT SERVICE_DATE_TO FROM QMS_GIC_LIFECYCLE WHERE QUALITY_MEASURE_ID = ? AND MEMBER_ID = ?),\r\n" + 
					    		"'Y',SYSDATE,SYSDATE,'Y','Y',SYSDATE,'Datawarehouse','Raghu')");
				qmsRecord.setString(1, qMSMemberReq.getCareGapsId());
				qmsRecord.setString(2, qMSMemberReq.getMemberId());
				qmsRecord.setString(3, qMSMemberReq.getMemberId());
				qmsRecord.setString(4, qMSMemberReq.getCareGapsId());
				qmsRecord.setString(5, qMSMemberReq.getMemberId());
				qmsRecord.setString(6, qMSMemberReq.getCareGapsId());
				qmsRecord.setString(7, qMSMemberReq.getCareGapsId());
				qmsRecord.setString(8, qMSMemberReq.getMemberId());
				qmsRecord.setString(9, qMSMemberReq.getStatus());
				qmsRecord.setString(10, qMSMemberReq.getInterventions());
				qmsRecord.setString(11, qMSMemberReq.getPriority());
				qmsRecord.setString(11, qMSMemberReq.getPayorComments());
				qmsRecord.setString(12, qMSMemberReq.getProviderComments());
				qmsRecord.setString(13, qMSMemberReq.getCareGapsId());
				qmsRecord.setString(14, qMSMemberReq.getMemberId());
				qmsRecord.setString(15, qMSMemberReq.getCareGapsId());
				qmsRecord.setString(16, qMSMemberReq.getMemberId());
				qmsRecord.setString(17, qMSMemberReq.getCareGapsId());
				qmsRecord.setString(18, qMSMemberReq.getMemberId());
				System.out.println("Validate the qms query : " + qmsRecord.toString());
				qmsRecord.executeUpdate();
//				String membergplistQry = "update table QMS_GIC_LIFECYCLE set INTERVENTIONS="+qmsGicLifecycle.getInterventions() != null ? qmsGicLifecycle.getInterventions() : "" +", PRIORITY="+qmsGicLifecycle.getPriority()+", PAYOR_COMMENTS="+qmsGicLifecycle.getPayorComments()+", PROVIDER_COMMENTS="+qmsGicLifecycle.getProviderComments()+", STATUS="+qmsGicLifecycle.getStatus()+" where QUALITY_MEASURE_ID="+qmsGicLifecycle.getQualityMeasureId();
//				statement.execute(membergplistQry);
			
			connection.commit();
		} catch (Exception e) {
			e.printStackTrace();
			throw new QmsInsertionException("Failed while inserting into QMSLifeCycle data", e);
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return qMSMemberReq;
	}

	private List<QmsGicLifecycle> getModifiedMeasureRecords(DimMemeberList dimMemberGaps1, String qmsMsgrIds) {
		List<QmsGicLifecycle> modifiedQmsLifeCycles = new ArrayList<QmsGicLifecycle>();
		List<String> qmsModifications = Arrays.asList(qmsMsgrIds.split(","));
		for (QmsGicLifecycle qmsLfCycle : dimMemberGaps1.getQmsGicLifecycle()) {
			if(qmsModifications.contains(qmsLfCycle.getQualityMeasureId()))
				modifiedQmsLifeCycles.add(qmsLfCycle);
		}
		return modifiedQmsLifeCycles;
	}

	@Override
	public List<MemberCareGapsList> findAllMembersList() {
		List<MemberCareGapsList> memberCareGapsList=new ArrayList<MemberCareGapsList>(); 
		Map<String, MemberCareGapsList> members = new HashMap<String, MemberCareGapsList>();
//		MemberCareGapsList memberCareGapsList = new MemberCareGapsList();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {

			connection = qmsConnection.getHiveConnection();
			System.out.println("Service after connection " + connection);
			statement = connection.createStatement();
			String memberCregapList = " SELECT CONCAT(DM.FIRST_NAME,' ',DM.MIDDLE_NAME,' ',DM.LAST_NAME) AS NAME, \r\n" + 
					"FLOOR(DATEDIFF('2018-12-31',(CONCAT(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4),'-',SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2),'-',SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2))))/365.25) AS AGE,\r\n" + 
					"DM.GENDER, CONCAT(DP.FIRST_NAME,' ',DP.LAST_NAME) AS PCP, DQM.MEASURE_TITLE AS CARE_GAPS,\r\n" + 
					"COUNT(DQM.MEASURE_TITLE) AS COUNT_OF_CARE_GAPS, DPP.PLAN_NAME AS PLAN, GIC.GAP_DATE AS TIME_PERIOD\r\n" + 
					"FROM QMS_GIC_LIFECYCLE GIC \r\n" + 
					"INNER JOIN DIM_MEMBER DM ON DM.MEMBER_ID = GIC.MEMBER_ID\r\n" + 
					"INNER JOIN FACT_MEM_ATTRIBUTION FMA ON FMA.MEMBER_SK = DM.MEMBER_SK\r\n" + 
					"INNER JOIN DIM_PROVIDER DP ON DP.PROVIDER_SK = FMA.PROVIDER_SK\r\n" + 
					"INNER JOIN DIM_QUALITY_MEASURE DQM ON DQM.QUALITY_MEASURE_ID = GIC.QUALITY_MEASURE_ID\r\n" + 
					"INNER JOIN DIM_PRODUCT_PLAN DPP ON DPP.PRODUCT_PLAN_ID = GIC.PRODUCT_PLAN_ID\r\n" + 
					"WHERE GIC.GAP_DATE < '25-JUN-18'\r\n" + 
					"GROUP BY CONCAT(DM.FIRST_NAME,' ',DM.MIDDLE_NAME,' ',DM.LAST_NAME), \r\n" + 
					"FLOOR(DATEDIFF('2018-12-31',(CONCAT(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4),'-',SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2),'-',SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2))))/365.25),\r\n" + 
					"DM.GENDER, CONCAT(DP.FIRST_NAME,' ',DP.LAST_NAME), DQM.MEASURE_TITLE, DPP.PLAN_NAME, GIC.GAP_DATE";

			resultSet = statement.executeQuery(memberCregapList);
			
			while (resultSet.next()) {
				String name =resultSet.getString("NAME");
				if(!members.containsKey(name)) {
					MemberCareGapsList memberCareGaps =new MemberCareGapsList();
					memberCareGaps.setName(name);
					memberCareGaps.setAge(resultSet.getString("AGE"));
					memberCareGaps.setGender(resultSet.getString("GENDER"));
					memberCareGaps.setRiskGrade("LOW");
					memberCareGaps.setCountOfCareGaps(1);
					//For care gaps
					MemberCareGaps memberCareGaps2 = new MemberCareGaps();
					memberCareGaps2.setPcp(resultSet.getString("PCP"));
					memberCareGaps2.setCareGaps(resultSet.getString("CARE_GAPS"));
					memberCareGaps2.setPlan(resultSet.getString("PLAN"));
					memberCareGaps2.setTimePeriod(resultSet.getString("TIME_PERIOD"));
					memberCareGaps.setMembers(new ArrayList<MemberCareGaps>());
					memberCareGaps.getMembers().add(memberCareGaps2);
					members.put(name, memberCareGaps);
				}
				else {
					MemberCareGaps memberCareGaps2 = new MemberCareGaps();
					memberCareGaps2.setPcp(resultSet.getString("PCP"));
					memberCareGaps2.setCareGaps(resultSet.getString("CARE_GAPS"));
					memberCareGaps2.setPlan(resultSet.getString("PLAN"));
					memberCareGaps2.setTimePeriod(resultSet.getString("TIME_PERIOD"));
					MemberCareGapsList memberCareGaps = members.get(name);
					memberCareGaps.getMembers().add(memberCareGaps2);
					int carGap = memberCareGaps.getCountOfCareGaps()+1;
					memberCareGaps.setRiskGrade(getRiskBasedOnCareGap(carGap));
					memberCareGaps.setCountOfCareGaps(carGap);
				}
			}
			if(members.size() > 0)
				memberCareGapsList.addAll(members.values());
			
			System.out.println("Service resultset" + resultSet.getFetchSize());
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}	
		
		
		return memberCareGapsList;
	}

	private String getRiskBasedOnCareGap(int carGap) {
		String risk = "LOW";
		if(carGap == 2 || carGap ==3) {
			risk = "MEDIUM";
		}else if(carGap > 3 && carGap < 7) {
			risk = "HIGH";
		}else if(carGap > 6) {
			risk = "CATASTROPHIC";
		}
		return risk;
	}

	@Override
	public List<DimMemberGapListSearch> findSearchMembergapList(String search) {
		List<DimMemberGapListSearch> dimMemberDetailLst = new ArrayList<DimMemberGapListSearch>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		
		try {

			connection = qmsConnection.getHiveConnection();
			System.out.println("Service after connection " + connection);
			statement = connection.createStatement();
			
			String membergaptotallistqry = "SELECT  DM.MEMBER_ID, DM.FIRST_NAME, DM.MIDDLE_NAME ,DM.LAST_NAME FROM DIM_MEMBER DM where DM.MEMBER_ID like '"+search+"%' or DM.FIRST_NAME like '"+search+"%' or DM.MIDDLE_NAME like '"+search+"%' or DM.LAST_NAME like '"+search+"%' order by DM.MEMBER_ID ASC";
			resultSet = statement.executeQuery(membergaptotallistqry);
			Map<String, DimMemberGapListSearch> dimMerberscerchlist = new TreeMap<String, DimMemberGapListSearch >();
			while (resultSet.next()) {
				DimMemberGapListSearch dimMemberDetails = new DimMemberGapListSearch();
				dimMemberDetails.setFirstName(resultSet.getString("FIRST_NAME"));
				dimMemberDetails.setMiddelName(resultSet.getString("MIDDLE_NAME"));
				dimMemberDetails.setLastName(resultSet.getString("LAST_NAME"));
				dimMemberDetails.setMemberId(resultSet.getString("MEMBER_ID"));
				dimMerberscerchlist.put(resultSet.getString("MEMBER_ID"), dimMemberDetails);
				
			}
			dimMemberDetailLst.addAll(dimMerberscerchlist.values());
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			qmsConnection.closeJDBCResources(resultSet, statement, connection);
		}
		return dimMemberDetailLst;
	}

}