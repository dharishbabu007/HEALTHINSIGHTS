--------------------------------------------------------
--  File created - Monday-May-28-2018   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table QMS_DATA_SOURCES
--------------------------------------------------------

  CREATE TABLE "ADMIN"."QMS_DATA_SOURCES" 
   (	"DATA_SOURCE_ID" VARCHAR2(100 BYTE), 
	"DATA_SOURCE_NAME" VARCHAR2(1500 BYTE), 
	"DATA_SOURCE_DESCRIPTION" VARCHAR2(1500 BYTE), 
	"REC_UPDATE_DATE" TIMESTAMP (6), 
	"CURRENT_FLAG" VARCHAR2(1 BYTE), 
	"CREATED_BY" VARCHAR2(100 BYTE)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "USERS" ;
REM INSERTING into ADMIN.QMS_DATA_SOURCES
SET DEFINE OFF;
Insert into ADMIN.QMS_DATA_SOURCES (DATA_SOURCE_ID,DATA_SOURCE_NAME,DATA_SOURCE_DESCRIPTION,REC_UPDATE_DATE,CURRENT_FLAG,CREATED_BY) values ('1','CSV','CSV',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y','raghu');
Insert into ADMIN.QMS_DATA_SOURCES (DATA_SOURCE_ID,DATA_SOURCE_NAME,DATA_SOURCE_DESCRIPTION,REC_UPDATE_DATE,CURRENT_FLAG,CREATED_BY) values ('2','Survey: Patient Reported Data/Survey','Survey: Patient Reported Data/Survey',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y','raghu');
Insert into ADMIN.QMS_DATA_SOURCES (DATA_SOURCE_ID,DATA_SOURCE_NAME,DATA_SOURCE_DESCRIPTION,REC_UPDATE_DATE,CURRENT_FLAG,CREATED_BY) values ('3','Registry','Registry',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y','raghu');
Insert into ADMIN.QMS_DATA_SOURCES (DATA_SOURCE_ID,DATA_SOURCE_NAME,DATA_SOURCE_DESCRIPTION,REC_UPDATE_DATE,CURRENT_FLAG,CREATED_BY) values ('4','Administrative Claims','Administrative Claims',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y','raghu');
Insert into ADMIN.QMS_DATA_SOURCES (DATA_SOURCE_ID,DATA_SOURCE_NAME,DATA_SOURCE_DESCRIPTION,REC_UPDATE_DATE,CURRENT_FLAG,CREATED_BY) values ('5','Electronic Clinical Data','Electronic Clinical Data',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y','raghu');
Insert into ADMIN.QMS_DATA_SOURCES (DATA_SOURCE_ID,DATA_SOURCE_NAME,DATA_SOURCE_DESCRIPTION,REC_UPDATE_DATE,CURRENT_FLAG,CREATED_BY) values ('6','Paper Medical Records','Paper Medical Records',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y','raghu');
Insert into ADMIN.QMS_DATA_SOURCES (DATA_SOURCE_ID,DATA_SOURCE_NAME,DATA_SOURCE_DESCRIPTION,REC_UPDATE_DATE,CURRENT_FLAG,CREATED_BY) values ('7','CMS Web Interface','CMS Web Interface',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y','raghu');
Insert into ADMIN.QMS_DATA_SOURCES (DATA_SOURCE_ID,DATA_SOURCE_NAME,DATA_SOURCE_DESCRIPTION,REC_UPDATE_DATE,CURRENT_FLAG,CREATED_BY) values ('8','Not Identified','Not Identified',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y','raghu');
--------------------------------------------------------
--  DDL for Index SYS_C0011557
--------------------------------------------------------

  CREATE UNIQUE INDEX "ADMIN"."SYS_C0011557" ON "ADMIN"."QMS_DATA_SOURCES" ("DATA_SOURCE_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "USERS" ;
--------------------------------------------------------
--  Constraints for Table QMS_DATA_SOURCES
--------------------------------------------------------

  ALTER TABLE "ADMIN"."QMS_DATA_SOURCES" MODIFY ("DATA_SOURCE_ID" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_DATA_SOURCES" MODIFY ("DATA_SOURCE_NAME" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_DATA_SOURCES" MODIFY ("REC_UPDATE_DATE" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_DATA_SOURCES" MODIFY ("CURRENT_FLAG" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_DATA_SOURCES" MODIFY ("CREATED_BY" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_DATA_SOURCES" ADD PRIMARY KEY ("DATA_SOURCE_ID")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "USERS"  ENABLE;
