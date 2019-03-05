package com.qms.rest.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name = "QMS_QUALITY_PROGRAM")
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class QualityProgram {

    @Id
   // @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "QUALITY_PROGRAM_ID")
    private int qualityProgramId;
    
    @Column(name = "PROGRAM_ID")
    private int programId;

    @Column(name = "PROGRAM_NAME")
    private String programName;

    @Column(name = "START_DATE")
    private Date startDate;

    @Column(name = "END_DATE")
    private Date endDate;

    @Column(name = "CATEGORY_NAME")
    private String categoryName;

    @Column(name = "MAX_POINTS")
    private int maxPoints;

    @Column(name = "MAX_SCORE")
    private int maxScore;

    @Column(name = "REC_CREATE_DATE")
    private Date recCreateDate;

    @Column(name = "REC_UPDATE_DATE")
    private Date recUpdateDate;

    @Column(name = "CURR_FLAG")
    private String currentFlag;

     @Column(name = "USER_NAME")
    private String user;

    @Column(name = "LATEST_FLAG")
    private String latestFlag;

    @Column(name = "ACTIVE_FLAG")
    private String activeFlag;

    @Column(name = "INGESTION_DATE")
    private Date ingestionDate;

    @Column(name = "SOURCE_NAME ")
    private String source;
}
