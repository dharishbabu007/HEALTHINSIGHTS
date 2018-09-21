package com.qms.rest.service;

import static org.springframework.transaction.annotation.Propagation.REQUIRED;

import java.text.Format;
import java.text.SimpleDateFormat;
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
import com.qms.rest.model.QualityProgram;
import com.qms.rest.model.QualityProgramUI;
import com.qms.rest.repository.ProgramRepository;

@Service
public class ProgramService {

    @Autowired
    private ProgramRepository programRepository;

    @Autowired
    private HttpSession httpSession;

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
}
