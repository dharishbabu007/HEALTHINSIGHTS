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
@Table(name = "QMS_FILE_UPLOAD")
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
    
    @Column(name = "USER_NAME")
    private String userName;
    
    @Column(name = "DATETIME")
    private Date dateTime;    
}
