package com.qms.rest.repository;

import com.qms.rest.model.QualityProgram;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import java.util.List;

@RepositoryRestResource
public interface ProgramRepository extends CrudRepository<QualityProgram, Integer> {

    @Query(value = "select * from QMS_QUALITY_PROGRAM where QUALITY_PROGRAM_ID in (select max(QUALITY_PROGRAM_ID) from QMS_QUALITY_PROGRAM)", nativeQuery = true)
    List<QualityProgram> getQualityProgramByMaxQualityProgramId();

    QualityProgram findQualityProgramByProgramNameAndCategoryName(@Param("programName") String programName, @Param("categoryName") String categoryName);
}
