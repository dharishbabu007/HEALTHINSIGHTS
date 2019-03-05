package com.qms.rest.service;

import static org.springframework.transaction.annotation.Propagation.REQUIRED;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.qms.rest.exception.ProgramCreatorException;
import com.qms.rest.model.Program;
import com.qms.rest.model.ProgramCategory;
import com.qms.rest.model.ProgramCategoryEdit;
import com.qms.rest.model.ProgramEdit;
import com.qms.rest.model.QualityProgram;
import com.qms.rest.model.QualityProgramUI;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RoleScreen;
import com.qms.rest.model.ScreenPermission;
import com.qms.rest.model.User;
import com.qms.rest.repository.ProgramRepository;
import com.qms.rest.util.QMSConnection;
import com.qms.rest.util.QMSConstants;
import com.qms.rest.util.QMSDateUtil;

@Service
public class ProgramService {

    @Autowired
    private ProgramRepository programRepository;

    @Autowired
    private HttpSession httpSession;
    
	@Autowired
	private QMSConnection qmsConnection;	
	
    private final String CURRENT_FLAG="Y";

    private static int nextQualityProgramId = 1050;
    private static int nextProgramId = 1050;

    private static QualityProgram qualityProgram = null;

    private static volatile boolean FETCHED_FROM_DB = false;

    @Transactional(propagation = REQUIRED)
    public void createProgram(Program program){
        System.out.println("About to invoke repository for program: "+program);
        buildQualityPrograms(program).stream().forEach(q-> programRepository.save(q));
    }
    
    public QualityProgramUI getProgramById(int programId) {
    	QualityProgramUI qualityProgramUI = null;
    	List<QualityProgram> qualityPrograms = programRepository.findQualityProgramByProgramId(programId);
    	if(qualityPrograms != null && qualityPrograms.size() > 0) {
    		QualityProgram qualityProgram = qualityPrograms.get(0);
    		qualityProgramUI = new QualityProgramUI();	
    		qualityProgramUI.setProgramId(qualityProgram.getProgramId());
    		qualityProgramUI.setProgramName(qualityProgram.getProgramName());    		
			Format formatter = new SimpleDateFormat("dd-MMM-yy");
			if(qualityProgram.getStartDate() != null)
				qualityProgramUI.setStartDate(formatter.format(qualityProgram.getStartDate()));
			if(qualityProgram.getEndDate() != null)
				qualityProgramUI.setEndDate(formatter.format(qualityProgram.getEndDate()));			
    	}
    	return qualityProgramUI;
    }    

    private List<QualityProgram> buildQualityPrograms(Program program) {
        return  program.getProgramCategorys().stream().map(category ->
                buildQualityProgram(program.getProgramName(), program.getStartDate(),program.getEndDate(), category))
                .collect(Collectors.toList());
    }

    private QualityProgram buildQualityProgram( String programName, Date startDate, Date endDate, ProgramCategory programCategory){
        String categoryName = programCategory.getCategoryName();

      if(startDate == null || endDate == null) {
    	  throw new ProgramCreatorException(" Start Date and End Date should not be null. ");
      }
      
      	if(isRecordAlreadyExist(programName, startDate, endDate)){            
            //throw new ProgramCreatorException("Program already exist for ProgramName: "+programName+" and category: "+categoryName);
            throw new ProgramCreatorException("Program already exist for ProgramName, Start Date and End Date");
        }

        if(qualityProgram ==  null && !FETCHED_FROM_DB){
            qualityProgram = programRepository.getQualityProgramByMaxQualityProgramId().get(0);
            nextProgramId = qualityProgram.getProgramId();;
            nextQualityProgramId = qualityProgram.getQualityProgramId();
            FETCHED_FROM_DB = true;
        }
        nextQualityProgramId = nextQualityProgramId + 1;
        nextProgramId = nextProgramId + 1;

        System.out.println("Latest qualityProgramId: " + nextQualityProgramId + " and ProgramId: " + nextProgramId);
        Date currentDate  = new Date();
        return QualityProgram.builder()
                 .qualityProgramId(nextQualityProgramId)
                .programId(nextProgramId)
                .programName(programName)
                .startDate(startDate)
                .endDate(endDate)
                .categoryName(categoryName)
                .maxPoints(programCategory.getMaxPoints())
                .maxScore(programCategory.getMaxScore())
                .recCreateDate(currentDate)
                .recUpdateDate(currentDate)
                .currentFlag(CURRENT_FLAG)
               //TODO need to take from session
                // .user(userData.isPresent()? userData.get().getName(): "Raghu")
                .user("Raghu")
                .latestFlag(CURRENT_FLAG)
                .activeFlag(CURRENT_FLAG)
                .ingestionDate(currentDate)
                .source("UI")
                .build();
    }

    private boolean isRecordAlreadyExist(String programName, Date startDate, Date endDate){
        List<QualityProgram> qualityPrograms = programRepository.findQualityProgramByProgramNameAndStartDateAndEndDate(programName, startDate, endDate);       
        System.out.println("Quality Program : "+qualityProgram);
        return qualityPrograms != null && qualityPrograms.size() > 0 ? true : false;
    }
    
    public ProgramEdit getProgramByName(String programName) {
        
    	ProgramEdit programEdit = null;
        Statement statement = null;
        ResultSet resultSet = null;         
        Connection connection = null;
        try {                               
              connection = qmsConnection.getOracleConnection();
              statement = connection.createStatement();
              resultSet = statement.executeQuery("select * from QMS_QUALITY_PROGRAM where PROGRAM_NAME='"+programName+"'");  
              
              ProgramCategoryEdit programCategory = null;
              List<ProgramCategoryEdit> programCategoryList = new ArrayList<>();
              while (resultSet.next()) {
            	  if(programEdit == null) {
	            	  programEdit = new ProgramEdit();
	            	  programEdit.setProgramId(resultSet.getInt("PROGRAM_ID"));
	            	  programEdit.setProgramName(resultSet.getString("PROGRAM_NAME"));
	            	  programEdit.setStartDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("START_DATE")));
	            	  programEdit.setEndDate(QMSDateUtil.getSQLDateFormat(resultSet.getDate("END_DATE")));
            	  }
	              
	              programCategory =  new ProgramCategoryEdit();
	              programCategory.setCategoryName(resultSet.getString("CATEGORY_NAME"));
	              programCategory.setMaxPoints(resultSet.getInt("MAX_POINTS"));
	              programCategory.setMaxScore(resultSet.getInt("MAX_SCORE"));
	              programCategoryList.add(programCategory);
              }
              if(programEdit != null)
            	  programEdit.setProgramCategorys(programCategoryList);
        } catch (Exception e) {
              e.printStackTrace();
        }
        finally {
              qmsConnection.closeJDBCResources(resultSet, statement, connection);
        }                 
        return programEdit;          
    }
    
	public RestResult editProgram(ProgramEdit program) {
		PreparedStatement statement = null;
		Connection connection = null;
		RestResult restResult = null;
		Statement statementObj = null;
		ResultSet resultSet = null;
		
		User userData = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		try {
			connection = qmsConnection.getOracleConnection();
			connection.setAutoCommit(false);
			
			List<ProgramCategoryEdit> programCategoryList =  program.getProgramCategorys();
			if(programCategoryList ==null || programCategoryList.isEmpty()) {
				ProgramCategoryEdit programCategoryEdit = new ProgramCategoryEdit();
				if(programCategoryList == null) programCategoryList = new ArrayList<>();
				programCategoryList.add(programCategoryEdit);
			}
			
			statementObj = connection.createStatement();
			statementObj.executeUpdate("delete from QMS_QUALITY_PROGRAM where PROGRAM_NAME='"+program.getProgramName()+"'");			
			
			int maxQualityProgramId = 0; 
			int maxProgramId = 0;
			resultSet = statementObj.executeQuery("select max(QUALITY_PROGRAM_ID) from QMS_QUALITY_PROGRAM");
			if (resultSet.next()) {
				maxQualityProgramId = resultSet.getInt(1);
			}						
			resultSet.close();
			
			resultSet = statementObj.executeQuery("select max(PROGRAM_ID) from QMS_QUALITY_PROGRAM");
			if (resultSet.next()) {
				maxProgramId = resultSet.getInt(1);
			}								
			
			maxProgramId = maxProgramId+1;
			String sqlStatementInsert = "insert into QMS_QUALITY_PROGRAM(QUALITY_PROGRAM_ID,PROGRAM_ID,"
					+ "PROGRAM_NAME,START_DATE,END_DATE,CATEGORY_NAME,MAX_POINTS,MAX_SCORE,"
					+ "CURR_FLAG,REC_CREATE_DATE,REC_UPDATE_DATE,LATEST_FLAG,ACTIVE_FLAG,INGESTION_DATE,SOURCE_NAME,USER_NAME) "
					+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			statement = connection.prepareStatement(sqlStatementInsert);
			
			for (ProgramCategoryEdit programCategoryEdit : programCategoryList) {
				int i=0;
				maxQualityProgramId++;
				System.out.println(" Adding QMS_QUALITY_PROGRAM with id --> " + maxQualityProgramId);
				statement.setInt(++i, maxQualityProgramId);
				statement.setInt(++i, maxProgramId);
				statement.setString(++i, program.getProgramName());
				statement.setString(++i, program.getStartDate());
				statement.setString(++i, program.getEndDate());
				statement.setString(++i, programCategoryEdit.getCategoryName());
				statement.setInt(++i, programCategoryEdit.getMaxPoints());
				statement.setInt(++i, programCategoryEdit.getMaxScore());
				
				Date date = new Date();				
				Timestamp timestamp = new Timestamp(date.getTime());				
				statement.setString(++i, "Y");
				statement.setTimestamp(++i, timestamp);
				statement.setTimestamp(++i, timestamp);
				statement.setString(++i, "Y");
				statement.setString(++i, "A");
				statement.setTimestamp(++i, timestamp);
				statement.setString(++i, QMSConstants.MEASURE_SOURCE_NAME);				
				
				if(userData == null || userData.getId() == null)
					statement.setString(++i, QMSConstants.MEASURE_USER_NAME);
				else
					statement.setString(++i, userData.getLoginId());
				
				statement.addBatch();
			}
			int [] rowsAdded = statement.executeBatch();
			connection.commit();
			System.out.println(" Rows added   --> " + rowsAdded!=null?rowsAdded.length:0);
			restResult = RestResult.getSucessRestResult("Program edit success.");
		} catch (Exception e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {				
				e1.printStackTrace();
			}
			restResult = RestResult.getFailRestResult(e.getMessage());
			e.printStackTrace();
		}
		finally {			
			qmsConnection.closeJDBCResources(resultSet, statementObj, null);
			qmsConnection.closeJDBCResources(null, statement, connection);
		}	
		return restResult;
	}    
    
    
}
