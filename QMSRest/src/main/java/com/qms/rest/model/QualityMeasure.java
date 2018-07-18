package com.qms.rest.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Entity
@Table(name ="DIM_QUALITY_MEASURE")
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QualityMeasure {

    @Id
    @Column(name="QUALITY_MEASURE_ID")
    private int qualityMeasureId;

    @Column(name = "QUALITY_MEASURE_SK")
    private String qualityMeasureSk;

    @Column(name = "QUALITY_PROGRAM_SK")
    private String qualityProgramSk;

    @Column(name = "MEASURE_TITLE")
    private String measureTitle;

    @Column(name = "DESCRIPTION")
    private String description;

    @Column(name = "TARGET_POPULATION_AGE")
    private String targetPopulationAge;

    @Column(name = "NUMERATOR")
    private String numerator;

    @Column(name = "DENOMINATOR")
    private String denominator;

    @Column(name = "DENO_EXCLUSIONS")
    private String denoExlusions;

    @Column(name = "NUM_EXCLUSIONS")
    private String numExclusions;

    @Column(name = "DENO_EXCEPTIONS")
    private String denoExceptions;

    @Column(name = "NUM_EXCEPTIONS")
    private String numExceptions;

    @Column(name = "NQS_DOMAIN")
    private String nqsDomain;

    @Column(name = "TYPE")
    private String type;

    @Column(name = "CLINICAL_CONDITIONS")
    private String clinicalConditions;

    @Column(name = "SUBCONDITIONS")
    private String subConditions;

    @Column(name = "STEWARD")
    private String steward;

    @Column(name = "DATA_SOURCES")
    private String dataSources;

    @Column(name = "MEASURE_GROUPS")
    private String measureGroups;

    @Column(name = "CARE_SETTINGS")
    private String careSettings;

    @Column(name = "REPORTING_LEVEL")
    private String reportingLevel;

    @Column(name = "IS_ACTIVE")
    private String isActive;

    @Column(name = "PRIORITY")
    private String priority;

    @Column(name = "START_DATE_SK")
    private String startDateSk;

    @Column(name = "END_DATE_SK")
    private String endDateSk;

    @Column(name = "DECILE1_START")
    private float decie1Start;

    @Column(name = "DECILE1_END")
    private float decile1End;

    @Column(name = "DECILE2_START")
    private float decile2Start;

    @Column(name = "DECILE2_END")
    private float decile2End;

    @Column(name = "DECILE3_START")
    private float decile3Start;

    @Column(name = "DECILE3_END")
    private float decile3End;

    @Column(name = "DECILE4_START")
    private float decile4Start;

    @Column(name = "DECILE4_END")
    private float decile4End;

    @Column(name = "DECILE5_START")
    private float decile5Start;

    @Column(name = "DECILE5_END")
    private float decile5End;

    @Column(name = "DECILE6_START")
    private float decile6Start;

    @Column(name = "DECILE6_End")
    private float decile6End;

    @Column(name = "DECILE7_START")
    private float decile7Start;

    @Column(name = "DECILE7_END")
    private float decile7End;

    @Column(name = "DECILE8_START")
    private float decile8Start;

    @Column(name = "DECILE8_END")
    private float decile8End;

    @Column(name = "DECILE9_START")
    private float decile9Start;

    @Column(name = "DECILE9_END")
    private float decile9End;

    @Column(name = "DECILE10_START")
    private float decile10Start;

    @Column(name = "DECILE10_END")
    private float decile10End;

    @Column(name = "CURR_FLAG")
    private String cuurentFlag;

    @Column(name ="REC_CREATE_DATE")
    private Date recCreateDate;

    @Column(name ="REC_UPDATE_DATE")
    private Date recUpdateDate;

    @Column(name = "LATEST_FLAG")
    private String latestFlag;

    @Column(name = "ACTIVE_FLAG")
    private String activeFlag;

    @Column(name ="INGESTION_DATE")
    private Date ingestionDate;

    @Column(name = "SOURCE")
    private String source;

    @Column(name = "USER")
    private String user;
}
