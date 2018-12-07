package com.qms.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.qms.rest.exception.QmsInsertionException;
import com.qms.rest.model.DimMemberGapListSearch;
import com.qms.rest.model.DimMemeberList;
import com.qms.rest.model.FactHedisGapsInCare;
import com.qms.rest.model.MemberCareGaps;
import com.qms.rest.model.MemberCareGapsList;
import com.qms.rest.model.QMSMemberReq;
import com.qms.rest.model.QmsGicLifecycle;
import com.qms.rest.util.QMSConnection;

@Service("memberGapList")
public class MemberGapListServiceImpl implements MemberGapListService {

	
	
	@Autowired
	private QMSConnection qmsConnection;
	
	
	/*Screen Number 7- Member Gaps List*/
	@Override
	public DimMemeberList findMembergapListByMid(String mid) {
		DimMemeberList dimMemeberList = new DimMemeberList();
		Map<String, QmsGicLifecycle> qmsGicLifyCycleMap = new HashMap<String, QmsGicLifecycle>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {

			connection = qmsConnection.getOracleConnection();
			System.out.println("Service after connection " + connection);
			statement = connection.createStatement();
			//Hive query
//			String membergplistQry = "SELECT DM.MEMBER_ID, CONCAT(DM.FIRST_NAME,' ',DM.LAST_NAME)  AS NAME, DM.GENDER, DD.DATE_SK, DD.CALENDAR_DATE AS DATE_OF_BIRTH, GIC1.QUALITY_MEASURE_ID, MAX(GIC1.INTERVENTIONS) AS INTERVENTIONS, MAX(GIC1.STATUS) AS STATUS, QM.MEASURE_TITLE, MIN(GIC1.GAP_DATE) AS START_DATE, MAX(GIC2.GAP_DATE) AS END_DATE, DATEDIFF(CURRENT_DATE(),MAX(FROM_UNIXTIME(UNIX_TIMESTAMP(GIC2.GAP_DATE,'dd-MMM-yy'),'yyyy-MM-dd'))) AS DURATION\n" + 
//					"FROM DIM_MEMBER DM\n" + 
//					"INNER JOIN QMS_GIC_LIFECYCLE GIC1 ON GIC1.MEMBER_ID = DM.MEMBER_ID\n" + 
//					"LEFT OUTER JOIN QMS_GIC_LIFECYCLE GIC2 ON GIC1.QUALITY_MEASURE_ID = GIC2.QUALITY_MEASURE_ID AND GIC1.MEMBER_ID = GIC2.MEMBER_ID\n" + 
//					"INNER JOIN DIM_QUALITY_MEASURE QM ON GIC1.QUALITY_MEASURE_ID=QM.QUALITY_MEASURE_ID\n" + 
//					"INNER JOIN DIM_DATE DD ON DD.DATE_SK = DM.DATE_OF_BIRTH_SK where dm.member_id="+mid+"\n" + 
//					"GROUP BY DM.MEMBER_ID, CONCAT(DM.FIRST_NAME,' ',DM.LAST_NAME), DM.GENDER, DD.DATE_SK, DD.CALENDAR_DATE, GIC1.QUALITY_MEASURE_ID, QM.MEASURE_TITLE";
			
			//oracle query
			
			/*
			String membergplistQry = "SELECT DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME) AS NAME, DM.GENDER, DD.DATE_SK," 
			+"(TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')) AS DATE_OF_BIRTH," 
			+"GIC1.QUALITY_MEASURE_ID, MAX(GIC1.INTERVENTIONS) AS INTERVENTIONS, MAX(GIC1.STATUS) AS STATUS, QM.MEASURE_TITLE, MIN(GIC1.GAP_DATE) AS START_DATE, MAX(GIC2.GAP_DATE) AS END_DATE," 
			+"FLOOR(SYSDATE - MAX(GIC2.GAP_DATE)) AS DURATION,GIC1.PRIORITY AS PRIORITY "
			+"FROM DIM_MEMBER DM "
			+"INNER JOIN QMS_GIC_LIFECYCLE GIC1 ON GIC1.MEMBER_ID = DM.MEMBER_ID "
			+"LEFT OUTER JOIN QMS_GIC_LIFECYCLE GIC2 ON GIC1.QUALITY_MEASURE_ID = GIC2.QUALITY_MEASURE_ID AND GIC1.MEMBER_ID = GIC2.MEMBER_ID "
			+"INNER JOIN DIM_QUALITY_MEASURE QM ON GIC1.QUALITY_MEASURE_ID=QM.QUALITY_MEASURE_ID "
			+"INNER JOIN DIM_DATE DD ON DD.DATE_SK = DM.DATE_OF_BIRTH_SK "
			+"WHERE DM.MEMBER_ID='"+mid+"' " 
			+"GROUP BY DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME), DM.GENDER, DD.DATE_SK,GIC1.PRIORITY," 
			+"(TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')), GIC1.QUALITY_MEASURE_ID, QM.MEASURE_TITLE";
			*/
			
			/*
			String membergplistQry = "SELECT DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME) AS NAME, DM.GENDER, DD.DATE_SK,"
			+"(TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')) AS DATE_OF_BIRTH,"
			+"GIC1.QUALITY_MEASURE_ID, (GIC1.INTERVENTIONS) AS INTERVENTIONS, GIC1.STATUS AS STATUS, QM.MEASURE_TITLE, MIN(GIC1.GAP_DATE) AS START_DATE, MAX(GIC2.GAP_DATE) AS END_DATE,"
			+"FLOOR(SYSDATE - MAX(CAST(GIC1.GAP_DATE AS DATE))) AS DURATION, GIC1.PRIORITY AS PRIORITY " 
			+"FROM DIM_MEMBER DM "
			+"INNER JOIN QMS_GIC_LIFECYCLE GIC1 ON GIC1.MEMBER_ID = DM.MEMBER_ID " 
			+"LEFT OUTER JOIN QMS_GIC_LIFECYCLE GIC2 ON GIC1.QUALITY_MEASURE_ID = GIC2.QUALITY_MEASURE_ID AND GIC1.MEMBER_ID = GIC2.MEMBER_ID " 
			+"INNER JOIN DIM_QUALITY_MEASURE QM ON GIC1.QUALITY_MEASURE_ID=QM.QUALITY_MEASURE_ID " 
			+"INNER JOIN DIM_DATE DD ON DD.DATE_SK = DM.DATE_OF_BIRTH_SK " 
			+"WHERE DM.MEMBER_ID='"+mid+"' " 
			+"GROUP BY DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME), DM.GENDER, DD.DATE_SK, GIC1.PRIORITY,GIC1.INTERVENTIONS, GIC1.STATUS,"
			+"(TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')), GIC1.QUALITY_MEASURE_ID, QM.MEASURE_TITLE " 
			+"order by max(GIC1.GAP_DATE) desc";
			*/			
			
			String membergplistQry = "SELECT * FROM FINDMEMGAPLISTBYMID WHERE MEMBER_ID = '"+mid+"' ORDER BY START_DATE DESC";
			
			resultSet = statement.executeQuery(membergplistQry);
			if (resultSet.next()) {
				dimMemeberList.setMemberId(resultSet.getString("MEMBER_ID"));
				dimMemeberList.setName(resultSet.getString("name"));
				dimMemeberList.setGender(resultSet.getString("gender") != null ? resultSet.getString("gender").trim() : null);
				dimMemeberList.setDateOfBirthSk(resultSet.getString("DATE_OF_BIRTH"));
				dimMemeberList.setQmsGicLifecycle(new ArrayList<QmsGicLifecycle>());

				QmsGicLifecycle qmsGicLifecycle = new QmsGicLifecycle();
				qmsGicLifecycle.setInterventions(resultSet.getString("INTERVENTIONS"));
				qmsGicLifecycle.setPriority(resultSet.getString("PRIORITY"));
				qmsGicLifecycle.setDuration(resultSet.getString("DURATION"));
				qmsGicLifecycle.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
				qmsGicLifecycle.setStart_date(resultSet.getString("START_DATE"));
				qmsGicLifecycle.setEnd_date(resultSet.getString("END_DATE"));
				//qmsGicLifecycle.setPayorComments(resultSet.getString("PAYOR_COMMENTS"));
				qmsGicLifecycle.setStatus(resultSet.getString("STATUS"));
				qmsGicLifyCycleMap.put(resultSet.getString("QUALITY_MEASURE_ID"), qmsGicLifecycle);
				
				FactHedisGapsInCare factHedisGapsInCare = new FactHedisGapsInCare();
				factHedisGapsInCare.setMeasureTitle(resultSet.getString("MEASURE_TITLE"));
				factHedisGapsInCare.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
			//	factHedisGapsInCare.setStart_date(resultSet.getString("START_DATE"));
				//factHedisGapsInCare.setEnd_date(resultSet.getString("END_DATE"));
			//	factHedisGapsInCare.setDuration(resultSet.getString("DURATION"));
				qmsGicLifecycle.setFactHedisGapsInCare(new ArrayList<FactHedisGapsInCare>());
				qmsGicLifecycle.getFactHedisGapsInCare().add(factHedisGapsInCare);

			}

			while (resultSet.next()) {
				String qmsMesureId = resultSet.getString("QUALITY_MEASURE_ID");
				if(qmsGicLifyCycleMap.containsKey(qmsMesureId)) {
//					FactHedisGapsInCare factHedisGapsInCare = new FactHedisGapsInCare();
//					factHedisGapsInCare.setMeasureTitle(resultSet.getString("MEASURE_TITLE"));
//					factHedisGapsInCare.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
		//			factHedisGapsInCare.setStart_date(resultSet.getString("START_DATE"));
		//			factHedisGapsInCare.setEnd_date(resultSet.getString("END_DATE"));
			//		factHedisGapsInCare.setDuration(resultSet.getString("DURATION"));
				//	qmsGicLifyCycleMap.get(qmsMesureId).getFactHedisGapsInCare().add(factHedisGapsInCare);
				}else {
					QmsGicLifecycle qmsGicLifecycle = new QmsGicLifecycle();
					qmsGicLifecycle.setInterventions(resultSet.getString("INTERVENTIONS"));
					qmsGicLifecycle.setPriority(resultSet.getString("PRIORITY"));
				//	qmsGicLifecycle.setPayorComments(resultSet.getString("PAYOR_COMMENTS"));
					qmsGicLifecycle.setStatus(resultSet.getString("STATUS"));
					qmsGicLifecycle.setDuration(resultSet.getString("DURATION"));
					qmsGicLifecycle.setStart_date(resultSet.getString("START_DATE"));
					qmsGicLifecycle.setEnd_date(resultSet.getString("END_DATE"));
					qmsGicLifecycle.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
					qmsGicLifyCycleMap.put(resultSet.getString("QUALITY_MEASURE_ID"), qmsGicLifecycle);
					
					FactHedisGapsInCare factHedisGapsInCare = new FactHedisGapsInCare();
					factHedisGapsInCare.setMeasureTitle(resultSet.getString("MEASURE_TITLE"));
					factHedisGapsInCare.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
				//	factHedisGapsInCare.setStart_date(resultSet.getString("START_DATE"));
			//		factHedisGapsInCare.setEnd_date(resultSet.getString("END_DATE"));
			//		factHedisGapsInCare.setDuration(resultSet.getString("DURATION"));
					qmsGicLifecycle.setFactHedisGapsInCare(new ArrayList<FactHedisGapsInCare>());
					qmsGicLifecycle.getFactHedisGapsInCare().add(factHedisGapsInCare);
				}

			}
			
			if(qmsGicLifyCycleMap.size() > 0) {
				dimMemeberList.getQmsGicLifecycle().addAll(qmsGicLifyCycleMap.values());
			}

	
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
			connection = qmsConnection.getOracleConnection();
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
	public List<DimMemberGapListSearch> findSearchMembergapList(String search) {
		List<DimMemberGapListSearch> dimMemberDetailLst = new ArrayList<DimMemberGapListSearch>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		
		try {

			connection = qmsConnection.getHiveThriftConnection();
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
	
	
	
	
	/*Screen Number 13 - Member Care Gaps Registry*/
	@Override
	public List<MemberCareGapsList> findAllMembersList() {
		List<MemberCareGapsList> memberCareGapsList=new ArrayList<MemberCareGapsList>(); 
		Map<String, MemberCareGapsList> members = new HashMap<String, MemberCareGapsList>();
		Statement statement = null;
		ResultSet resultSet = null;
		Connection connection = null;
		try {

			connection = qmsConnection.getOracleConnection();
			System.out.println("Service after connection " + connection);
			statement = connection.createStatement();
			
			//hive query
//			String memberCregapList = "SELECT GIC.MEMBER_ID, CONCAT(DM.FIRST_NAME,' ',DM.MIDDLE_NAME,' ',DM.LAST_NAME) AS NAME, \n" + 
//					"FLOOR(DATEDIFF('2018-12-31',(CONCAT(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4),'-',SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2),'-',SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2))))/365.25) AS AGE,\n" + 
//					"DM.GENDER, CONCAT(DP.FIRST_NAME,' ',DP.LAST_NAME) AS PCP, DQM.MEASURE_TITLE AS CARE_GAPS, GIC.STATUS,\n" + 
//					"COUNT(DQM.MEASURE_TITLE) AS COUNT_OF_CARE_GAPS, DPP.PLAN_NAME AS PLAN, GIC.GAP_DATE AS TIME_PERIOD\n" + 
//					"FROM QMS_GIC_LIFECYCLE GIC \n" + 
//					"INNER JOIN DIM_MEMBER DM ON DM.MEMBER_ID = GIC.MEMBER_ID\n" + 
//					"INNER JOIN FACT_MEM_ATTRIBUTION FMA ON FMA.MEMBER_SK = DM.MEMBER_SK\n" + 
//					"INNER JOIN DIM_PROVIDER DP ON DP.PROVIDER_SK = FMA.PROVIDER_SK\n" + 
//					"INNER JOIN DIM_QUALITY_MEASURE DQM ON DQM.QUALITY_MEASURE_ID = GIC.QUALITY_MEASURE_ID\n" + 
//					"INNER JOIN DIM_PRODUCT_PLAN DPP ON DPP.PRODUCT_PLAN_ID = GIC.PRODUCT_PLAN_ID\n" + 
//					"WHERE GIC.GAP_DATE <= upper(from_unixtime(unix_timestamp(current_date, 'yyyy-MM-dd'),'d-MMM-yy')) \n" + 
//					"GROUP BY GIC.MEMBER_ID, CONCAT(DM.FIRST_NAME,' ',DM.MIDDLE_NAME,' ',DM.LAST_NAME), \n" + 
//					"FLOOR(DATEDIFF('2018-12-31',(CONCAT(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4),'-',SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2),'-',SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2))))/365.25),\n" + 
//					"DM.GENDER, CONCAT(DP.FIRST_NAME,' ',DP.LAST_NAME), DQM.MEASURE_TITLE, GIC.STATUS, DPP.PLAN_NAME, GIC.GAP_DATE";
			
			//oracle query
			String memberCregapList = "SELECT DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME) AS NAME, DM.GENDER,"
			+"FLOOR(TRUNC(CAST('31-DEC-18' AS DATE) - (TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')))/365.25) AS AGE,"
			+"(DP.FIRST_NAME||' '||DP.LAST_NAME) AS PCP, DQM.MEASURE_TITLE AS CARE_GAPS, GIC.STATUS,GIC.QUALITY_MEASURE_ID,"
			+"COUNT(DQM.MEASURE_TITLE) AS COUNT_OF_CARE_GAPS, DPP.PLAN_NAME AS PLAN, GIC.GAP_DATE AS TIME_PERIOD, GIC.COMPLIANCE_POTENTIAL "
			+"FROM QMS_GIC_LIFECYCLE GIC "
			+"INNER JOIN DIM_MEMBER DM ON DM.MEMBER_ID = GIC.MEMBER_ID "
			+"INNER JOIN FACT_MEM_ATTRIBUTION FMA ON FMA.MEMBER_SK = DM.MEMBER_SK "
			+"INNER JOIN DIM_PROVIDER DP ON DP.PROVIDER_SK = FMA.PROVIDER_SK "
			+"INNER JOIN DIM_QUALITY_MEASURE DQM ON DQM.QUALITY_MEASURE_ID = GIC.QUALITY_MEASURE_ID "
			+"INNER JOIN DIM_PRODUCT_PLAN DPP ON DPP.PRODUCT_PLAN_ID = GIC.PRODUCT_PLAN_ID "
			+"WHERE GIC.GAP_DATE <= SYSDATE "
			+"GROUP BY DM.MEMBER_ID, (DM.FIRST_NAME||' '||DM.MIDDLE_NAME||' '||DM.LAST_NAME), DM.GENDER,GIC.COMPLIANCE_POTENTIAL,"
			+"FLOOR(TRUNC(CAST('31-DEC-18' AS DATE) - (TO_DATE(SUBSTR(DM.DATE_OF_BIRTH_SK, 1, 4) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 5,2) || '-' || SUBSTR(DM.DATE_OF_BIRTH_SK, 7,2),'YYYY-MM-DD')))/365.25),"
			+"(DP.FIRST_NAME||' '||DP.LAST_NAME), DQM.MEASURE_TITLE, GIC.STATUS, DPP.PLAN_NAME, GIC.GAP_DATE,GIC.QUALITY_MEASURE_ID order by GIC.GAP_DATE DESC";
			
			//String memberCregapList = "SELECT * FROM FINDMEMGAPLISTFORALL ORDER BY TIME_PERIOD DESC";
			
			resultSet = statement.executeQuery(memberCregapList);
			System.out.println("Service resultset" + resultSet.getFetchSize());
			Map<String, Set<Integer>> countCareGapMap = new HashMap<>(); 
			while (resultSet.next()) {
				String member_id =resultSet.getString("MEMBER_ID");
				String quaMesid =resultSet.getString("QUALITY_MEASURE_ID");
				String mapKey = member_id+"##"+quaMesid; 
				
				Set<Integer> countHashSet = countCareGapMap.get(member_id);
				if(countHashSet == null) {
					countHashSet = new HashSet<>();	
					countCareGapMap.put(member_id, countHashSet);
				}
				countHashSet.add(Integer.parseInt(quaMesid));
					
				if(!members.containsKey(mapKey)) {
					MemberCareGapsList memberCareGaps =new MemberCareGapsList();
					memberCareGaps.setMember_id(member_id);
					memberCareGaps.setAge(resultSet.getString("AGE"));
					memberCareGaps.setGender(resultSet.getString("GENDER"));
					memberCareGaps.setName(resultSet.getString("NAME"));
					memberCareGaps.setRiskGrade("LOW");
					memberCareGaps.setCompliancePotential(resultSet.getString("COMPLIANCE_POTENTIAL"));
					memberCareGaps.setCountOfCareGaps(1);
					//For care gaps
					MemberCareGaps memberCareGaps2 = new MemberCareGaps();
					memberCareGaps2.setPcp(resultSet.getString("PCP"));
					memberCareGaps2.setCareGaps(resultSet.getString("CARE_GAPS"));
					memberCareGaps2.setPlan(resultSet.getString("PLAN"));
					memberCareGaps2.setStatus(resultSet.getString("STATUS"));
					memberCareGaps2.setTimePeriod(resultSet.getString("TIME_PERIOD"));
					memberCareGaps2.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
					
					
					memberCareGaps.setMembers(new ArrayList<MemberCareGaps>());
					memberCareGaps.getMembers().add(memberCareGaps2);
					members.put(mapKey, memberCareGaps);
				}
				else {
//					MemberCareGaps memberCareGaps2 = new MemberCareGaps();
//					memberCareGaps2.setPcp(resultSet.getString("PCP"));
//					memberCareGaps2.setCareGaps(resultSet.getString("CARE_GAPS"));
//					memberCareGaps2.setPlan(resultSet.getString("PLAN"));
//					memberCareGaps2.setStatus(resultSet.getString("STATUS"));
//				    memberCareGaps2.setTimePeriod(resultSet.getString("TIME_PERIOD"));
//				    memberCareGaps2.setQualityMeasureId(resultSet.getString("QUALITY_MEASURE_ID"));
					MemberCareGapsList memberCareGaps = members.get(mapKey);
//					memberCareGaps.getMembers().add(memberCareGaps2);
//					int carGap = memberCareGaps.getCountOfCareGaps()+1;
					int carGap = countHashSet.size();
					memberCareGaps.setRiskGrade(getRiskBasedOnCareGap(carGap));
					memberCareGaps.setCountOfCareGaps(carGap);
				}
			}
			if(members.size() > 0) {
				memberCareGapsList.addAll(members.values());
				
				for (MemberCareGapsList memberCareGapsList2 : memberCareGapsList) {
					String memberId = memberCareGapsList2.getMember_id();
					Set<Integer> countMesIds = countCareGapMap.get(memberId);					
					int carGap = countMesIds!= null?countMesIds.size():0;
					memberCareGapsList2.setRiskGrade(getRiskBasedOnCareGap(carGap));
					memberCareGapsList2.setCountOfCareGaps(carGap);					
				}
			}
			
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

	
	
	

}
