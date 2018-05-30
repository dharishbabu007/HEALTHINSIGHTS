--------------------------------------------------------
--  File created - Monday-May-28-2018   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table QMS_MEASURE_CONFIGURATOR
--------------------------------------------------------

  CREATE TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" 
   (	"MEASURE_ID" VARCHAR2(4000 BYTE), 
	"CATEGORY" VARCHAR2(4000 BYTE), 
	"CATEGORY_LINE_ID" NUMBER(*,0), 
	"OPERATOR" VARCHAR2(4000 BYTE), 
	"BUSINESS_EXPRESSION" VARCHAR2(4000 BYTE), 
	"TECHNICAL_EXPRESSION" VARCHAR2(4000 BYTE), 
	"REMARKS" VARCHAR2(4000 BYTE), 
	"VERSION" NUMBER(*,0), 
	"STATUS" VARCHAR2(4000 BYTE), 
	"CURRENT_FLAG" VARCHAR2(1 BYTE), 
	"REC_UPDATE_DATE" TIMESTAMP (6), 
	"MODIFIED_BY" VARCHAR2(4000 BYTE)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "USERS" ;
REM INSERTING into ADMIN.QMS_MEASURE_CONFIGURATOR
SET DEFINE OFF;
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Denominator',1,'AND','business exp1','ICDDIM.RECORD_STATE_C','with version 1',1,'save','Y',to_timestamp('16-FEB-18 12.20.47.742000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Denominator',2,'AND','business exp2','DIM_PATIENT.PAT_MRN','with version 1',1,'save','Y',to_timestamp('16-FEB-18 12.20.47.743000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Numerator',1,'AND','Num Bus exp1','RADFACT.CURR_FLAG','with numerator version 1',1,'submit','Y',to_timestamp('16-FEB-18 12.27.20.643000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Numerator',2,'AND','Num Bus exp2','RADFACT.EXAM_END_DATE','with numerator version 1',1,'submit','Y',to_timestamp('16-FEB-18 12.27.20.643000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Denominator',1,'AND','business exp version2 111','ICDDIM.RECORD_STATE_C','with version 2',2,'save','Y',to_timestamp('16-FEB-18 12.29.55.792000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Denominator',2,'AND','business exp version2 222','DIM_PATIENT.PAT_MRN','with version 2',2,'save','Y',to_timestamp('16-FEB-18 12.29.55.792000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Denominator_Exclusion',1,'AND','Bus exp 1 for Denom Excl','FACT_DIABETES_SPECIALITY.RECENT_HBA1C','Denom exlcusion version 1',1,'submit','Y',to_timestamp('16-FEB-18 01.14.02.422000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Denominator_Exclusion',2,'OR','Bus exp 2 for Denom Excl','FACT_MIPS_SCORE.DATE_DIM','Denom exlcusion version 1',1,'submit','Y',to_timestamp('16-FEB-18 01.14.02.422000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Denominator_Exclusion',3,'UNION','Bus exp 3 for Denom Excl','FACT_MIPS_SCORE.SCORE','Denom exlcusion version 1',1,'submit','Y',to_timestamp('16-FEB-18 01.14.02.423000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Denominator',1,'AND','business exp version2 111','ICDDIM.RECORD_STATE_C','with version 3',3,'submit','Y',to_timestamp('16-FEB-18 01.15.30.791000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Denominator',2,'AND','business exp version2 222','DIM_PATIENT.PAT_MRN','with version 3',3,'submit','Y',to_timestamp('16-FEB-18 01.15.30.806000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('5971','Denominator',3,'AND','business exp version2 333','PROCEDUREDIM.IS_ACTIVE_YN','with version 3',3,'submit','Y',to_timestamp('16-FEB-18 01.15.30.806000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator',1,'AND','Denom 1','DIM_PATIENT.PAT_ID','version 1',1,'save','Y',to_timestamp('09-MAR-18 01.44.53.598000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator',2,'AND','Denom 2','DIM_QUALITY_PERCENTILE.CURR_FLAG','version 1',1,'save','Y',to_timestamp('09-MAR-18 01.44.53.598000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator',3,'AND','Denom 3','STROKEFACT.ENCOUNTER_LINE_ID','version 1',1,'save','Y',to_timestamp('09-MAR-18 01.44.53.598000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Numerator',1,'AND','Num ver1','DIM_QUALITY_PROGRAM.CURR_FLAG','Version 1',1,'submit','Y',to_timestamp('09-MAR-18 01.46.02.626000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Numerator',2,'OR','Num ver2','STROKEFACT.PAT_ARRIVAL_TIME','Version 1',1,'submit','Y',to_timestamp('09-MAR-18 01.46.02.626000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator',1,'AND','Denom 1','DIM_PATIENT.PAT_ID','version 1',2,'save','Y',to_timestamp('09-MAR-18 01.46.25.953000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator',2,'AND','Denom 2','DIM_QUALITY_PERCENTILE.CURR_FLAG','version 1',2,'save','Y',to_timestamp('09-MAR-18 01.46.25.953000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator',1,'AND','MIPS','PROGRAM_LIST.PROGRAM_NAME','version 2',4,'save','Y',to_timestamp('15-MAY-18 03.34.55.379000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator',2,'AND','18-75','DIM_AGEGROUP.AGE_GROUP','version 2',4,'save','Y',to_timestamp('15-MAY-18 03.34.55.379000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator',3,'AND','Male','DIM_PATIENT.PAT_GENDER','version 2',4,'save','Y',to_timestamp('15-MAY-18 03.34.55.379000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('2443','Denominator',1,'AND','Test Denom1 v1 OBS','OBSTETRICSFACT.BP_CRITERIA_MET','Version 1',1,'save','Y',to_timestamp('23-FEB-18 04.30.57.854000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('2443','Denominator',2,'AND','Test Denom2 v1 OLY','OLYWYDIM.RESOURCE_NAME','Version 1',1,'save','Y',to_timestamp('23-FEB-18 04.30.57.854000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator',1,'AND','Denom 11','DIM_PATIENT.PAT_ID','version 2',3,'submit','Y',to_timestamp('12-MAR-18 05.10.37.542000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator',2,'AND','Denom 21','DIM_QUALITY_PERCENTILE.CURR_FLAG','version 2',3,'submit','Y',to_timestamp('12-MAR-18 05.10.37.543000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator_Exclusion',1,'AND','Test after separate fileds','OLYWYDIM.CURR_FLAG','ver 1',1,'submit','Y',to_timestamp('16-MAY-18 04.44.47.956000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator_Exclusion',1,'AND','Test after separate fileds','OLYWYDIM.CURR_FLAG','ver 2',2,'submit','Y',to_timestamp('17-MAY-18 02.42.01.359000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator_Exclusion',2,'AND','Test 17 05','ICDDIM.DX_NAME','ver 2',2,'submit','Y',to_timestamp('17-MAY-18 02.42.01.359000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator_Exclusion',1,'AND','Test after separate fileds','OLYWYDIM.CURR_FLAG','ver 2',3,'submit','Y',to_timestamp('17-MAY-18 04.29.55.275000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Denominator_Exclusion',2,'AND','Test 17 05','DIM_PATIENT.PAT_SSN','ver 2',3,'submit','Y',to_timestamp('17-MAY-18 04.29.55.275000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Numerator_Exclusion',1,'OR','Test Num','FACT_DIABETES_SPECIALITY.CURR_FLAG','Test Num01',1,'submit','Y',to_timestamp('17-MAY-18 04.30.40.403000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Numerator_Exclusion',1,'OR','Test Num','FACT_DIABETES_SPECIALITY.ENCOUNTER_LINE_ID','Test Num01',2,'submit','Y',to_timestamp('17-MAY-18 04.31.05.803000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
Insert into ADMIN.QMS_MEASURE_CONFIGURATOR (MEASURE_ID,CATEGORY,CATEGORY_LINE_ID,OPERATOR,BUSINESS_EXPRESSION,TECHNICAL_EXPRESSION,REMARKS,VERSION,STATUS,CURRENT_FLAG,REC_UPDATE_DATE,MODIFIED_BY) values ('10','Numerator_Exclusion',1,'OR','Test Num','FACT_DIABETES_SPECIALITY.FACILITY_ID','Test Num01',3,'submit','Y',to_timestamp('17-MAY-18 04.31.21.757000000 PM','DD-MON-RR HH.MI.SSXFF AM'),'2');
--------------------------------------------------------
--  Constraints for Table QMS_MEASURE_CONFIGURATOR
--------------------------------------------------------

  ALTER TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" MODIFY ("MEASURE_ID" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" MODIFY ("CATEGORY" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" MODIFY ("OPERATOR" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" MODIFY ("BUSINESS_EXPRESSION" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" MODIFY ("TECHNICAL_EXPRESSION" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" MODIFY ("REMARKS" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" MODIFY ("STATUS" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" MODIFY ("CURRENT_FLAG" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" MODIFY ("REC_UPDATE_DATE" NOT NULL ENABLE);
 
  ALTER TABLE "ADMIN"."QMS_MEASURE_CONFIGURATOR" MODIFY ("MODIFIED_BY" NOT NULL ENABLE);
