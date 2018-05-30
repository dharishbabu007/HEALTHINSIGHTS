--------------------------------------------------------
--  File created - Monday-May-28-2018   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table QMS_PROGRAM_LIST
--------------------------------------------------------

  CREATE TABLE "ADMIN"."QMS_PROGRAM_LIST" 
   (	"PROGRAM_ID" VARCHAR2(100 BYTE), 
	"PROGRAM_NAME" VARCHAR2(1000 BYTE), 
	"REC_CREATE_DATE" TIMESTAMP (6), 
	"REC_UPDATE_DATE" TIMESTAMP (6), 
	"ACTIVE_FLAG" VARCHAR2(1 BYTE)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "USERS" ;
REM INSERTING into ADMIN.QMS_PROGRAM_LIST
SET DEFINE OFF;
Insert into ADMIN.QMS_PROGRAM_LIST (PROGRAM_ID,PROGRAM_NAME,REC_CREATE_DATE,REC_UPDATE_DATE,ACTIVE_FLAG) values ('1','Merit-Based Incentive Payment System (MIPS) Program',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y');
Insert into ADMIN.QMS_PROGRAM_LIST (PROGRAM_ID,PROGRAM_NAME,REC_CREATE_DATE,REC_UPDATE_DATE,ACTIVE_FLAG) values ('2','Medicare Shared Savings Program',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y');
Insert into ADMIN.QMS_PROGRAM_LIST (PROGRAM_ID,PROGRAM_NAME,REC_CREATE_DATE,REC_UPDATE_DATE,ACTIVE_FLAG) values ('3','The Healthcare Effectiveness Data and Information Set (HEDIS)',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y');
Insert into ADMIN.QMS_PROGRAM_LIST (PROGRAM_ID,PROGRAM_NAME,REC_CREATE_DATE,REC_UPDATE_DATE,ACTIVE_FLAG) values ('4','Accountable Care Organization (ACO)',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y');
Insert into ADMIN.QMS_PROGRAM_LIST (PROGRAM_ID,PROGRAM_NAME,REC_CREATE_DATE,REC_UPDATE_DATE,ACTIVE_FLAG) values ('5','CMS Star',to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),to_timestamp('01-AUG-17 12.00.00.000000000 AM','DD-MON-RR HH.MI.SSXFF AM'),'Y');
--------------------------------------------------------
--  Constraints for Table QMS_PROGRAM_LIST
--------------------------------------------------------

  ALTER TABLE "ADMIN"."QMS_PROGRAM_LIST" MODIFY ("PROGRAM_ID" NOT NULL ENABLE);
