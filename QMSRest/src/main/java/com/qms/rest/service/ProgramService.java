package com.qms.rest.service;

import com.qms.rest.exception.ProgramCreatorException;
import com.qms.rest.model.Program;
import com.qms.rest.model.ProgramCategory;
import com.qms.rest.model.QualityProgram;
import com.qms.rest.model.User;
import com.qms.rest.repository.ProgramRepository;
import com.qms.rest.util.QMSConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpSession;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import static org.springframework.transaction.annotation.Propagation.REQUIRED;

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

    private List<QualityProgram> buildQualityPrograms(Program program) {
        return  program.getProgramCategorys().stream().map(category ->
                buildQualityProgram(program.getProgramName(), program.getStartDate(),program.getEndDate(), category))
                .collect(Collectors.toList());
    }

    private QualityProgram buildQualityProgram( String programName, Date startDate, Date endDate, ProgramCategory programCategory){
        String categoryName = programCategory.getCategoryName();

        Optional<User> userData = Optional.ofNullable((User)httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ));

      if(isRecordAlreadyExist(programName, categoryName)){
            System.out.println("Error: Program already exist for specified value");
            throw new ProgramCreatorException("Program already exist for ProgramName: "+programName+" and category: "+categoryName);
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

    private boolean isRecordAlreadyExist(String programName, String categoryName){
       QualityProgram qualityProgram = programRepository.findQualityProgramByProgramNameAndCategoryName(programName, categoryName);
        System.out.println("Quality Program : "+qualityProgram);
        return qualityProgram != null ? true : false;
    }
}
