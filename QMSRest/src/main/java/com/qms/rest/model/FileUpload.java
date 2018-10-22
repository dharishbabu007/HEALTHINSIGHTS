package com.qms.rest.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "QMS_FILE_INPUT")
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class FileUpload {

    @Id
    //@GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "FILE_ID")
    private int fileId;

    @Column(name = "FILE_NAME")
    private String fileName;	
    
    @Column(name = "USERS_NAME")
    private String usersName;
    
    @Column(name = "DATETIME")
    private Date dateTime;    
    
    @Column(name = "REC_CREATE_DATE")
    private Date recCreateDate;

    @Column(name = "REC_UPDATE_DATE")
    private Date recUpdateDate;

    @Column(name = "CURR_FLAG")
    private String currentFlag;

     @Column(name = "USER_NAME")
    private String userName;

    @Column(name = "LATEST_FLAG")
    private String latestFlag;

    @Column(name = "ACTIVE_FLAG")
    private String activeFlag;

    @Column(name = "INGESTION_DATE")
    private Date ingestionDate;

    @Column(name = "SOURCE_NAME")
    private String source;    
}
