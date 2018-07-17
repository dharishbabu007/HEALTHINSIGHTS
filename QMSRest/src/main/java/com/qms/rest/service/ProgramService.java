package com.qms.rest.service;

import com.qms.rest.exception.ProgramCreatorException;
import com.qms.rest.model.Program;
import com.qms.rest.model.ProgramCategory;
import com.qms.rest.model.QualityProgram;
import com.qms.rest.repository.ProgramRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import static org.springframework.transaction.annotation.Propagation.REQUIRED;

@Service
public class ProgramService {

    @Autowired
    private ProgramRepository programRepository;

    private final String CURRENT_FLAG="Y";

    private static int nextQualityProgramId = 0;
    private static int nextProgramId = 0;

    private static QualityProgram qualityProgram = null;

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

        if(isRecordAlreadyExist(programName, categoryName)){
            System.out.println("Error: Program already exist for specified value");
            throw new ProgramCreatorException("Program already exist for ProgramName: "+programName+" and category: "+categoryName);
        }

        if(qualityProgram ==  null){
            qualityProgram = programRepository.getQualityProgramByMaxQualityProgramId().get(0);
            nextProgramId = qualityProgram.getProgramId();;
            nextQualityProgramId = qualityProgram.getQualityProgramId();
        }
        nextQualityProgramId = nextQualityProgramId + 1;
        nextProgramId = nextProgramId + 1;

        System.out.println("Latest qualityProgramId: " + nextQualityProgramId + " and ProgramId: " + nextProgramId);

        return QualityProgram.builder()
                 .qualityProgramId(nextQualityProgramId)
                .programId(nextProgramId)
                .programName(programName)
                .startDate(startDate)
                .endDate(endDate)
                .categoryName(categoryName)
                .maxPoints(programCategory.getMaxPoints())
                .maxScore(programCategory.getMaxScore())
                .recCreateDate(new Date())
                .recUpdateDate(new Date())
                .currentFlag(CURRENT_FLAG)
                .modifiedBy("raghu")
                .build();
    }

    private boolean isRecordAlreadyExist(String programName, String categoryName){
       QualityProgram qualityProgram = programRepository.findQualityProgramByProgramNameAndCategoryName(programName, categoryName);
        System.out.println("Quality Program : "+qualityProgram);
        return qualityProgram != null? true : false;
    }
}
