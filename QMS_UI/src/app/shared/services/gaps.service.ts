import { CachedHttpClient } from './cache-httpclient';
import { Injectable } from '@angular/core';
import { environment } from '../../../environments/environment';
import { Observable,throwError } from 'rxjs';
import {  filter, map, catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../shared/services/http-error-handler.service';
import { PatScreen } from '../../layout/pat-screen/pat-screen.component';
import { Intervention } from '../../layout/health/interventions/interventions.component';
import { HttpClient, HttpHeaders } from '@angular/common/http';

const httpOptions = {
    headers: new HttpHeaders({
      'Access-Control-Allow-Origin': '*',
      'Content-Type':  'application/json'
    })
  };
@Injectable()
export class GapsService {
    private handleError: HandleError;
    constructor(private http: CachedHttpClient,  httpErrorHandler: HttpErrorHandler, private http1: HttpClient) {
        this.handleError =  httpErrorHandler.createHandleError();
    }

    getGaps(memberID) {
        return this.http.get(`http://healthinsight:8082/curis/memberGapList/member/${memberID}`);
    }
    getMemberGaps() {
        return this.http.get('http://healthinsight:8082/curis/memberGapList/findAllMembers');
    }
    getGapsInfo(gapId, memberId) {
        return this.http.get(`http://healthinsight:8082/curis/closeGaps/${memberId}/${gapId}`);
        // .pipe(
        //     map((items: any) => {
        //         return items.careGaps.filter(item => parseInt(gapId, 10)  === item.id );
        //       }, (error => error),
        //       )
        // );
    }
    getMemberList(query) {
        return this.http.get(`http://healthinsight:8082/curis/qms/members/${query}`);
    }
    getPrograms() {
        return this.http.get(`http://healthinsight:8082/curis/qms/qmshome_dropdown_list/QMS_MEASURE/PROGRAM_NAME`);
    }
    getDropDownPrograms() {
        return this.http.get(`http://healthinsight:8082/curis/qms/dropdown_namevalue_list/qms_quality_program/PROGRAM_ID/PROGRAM_NAME`);
    }
    getMeasureCategories() {
        return this.http.get(`http://healthinsight:8082/curis/qms/dropdown_namevalue_list/qms_quality_program/PROGRAM_NAME/CATEGORY_NAME`);
    }
    getMeasureTypes() {
        return this.http.get(`http://healthinsight:8082/curis/qms/dropdown_namevalue_list/QMS_MEASURE_TYPE/MEASURE_TYPE_ID/MEASURE_TYPE_NAME`);
    }
    getClinicalConditions() {
        return this.http.get(`http://healthinsight:8082/curis/qms/qmshome_dropdown_list/QMS_QUALITY_MEASURE/CLINICAL_CONDITIONS`);
    }
    getMeasureDomain() {
        return this.http.get(`http://healthinsight:8082/curis/qms/dropdown_namevalue_list/QMS_MEASURE_DOMAIN/MEASURE_DOMAIN_ID/MEASURE_DOMAIN_NAME`);
    }
    getWorkList() {
        return this.http.get(`http://healthinsight:8082/curis/qms/work_list/`);
    }
    getLibrary(programType, programValue) {
        return this.http.get(`http://healthinsight:8082/curis/qms/measure_list/${programType}/${programValue}`);
        
    }

    getCsv(){

    return this.http.get(`http://healthinsight:8082/curis/qms_file/csv_output`);
    
    }
    getCsv2(){
        return this.http.get('http://healthinsight:8082/curis/qms_file/csv_modelSummary');
    }
    getlheMeasureDetails() {
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhe_output`);
    }
    getCommunicationMeasureDetails(){
        return this.http.get('');
    }
    getLikehoodchurnMeasureDetails(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhc_member_list`);    
    }
    getSpv(memberId) {
        return this.http.get(`http://healthinsight:8082/curis/qms/spv/hedis/${memberId}`);
    }
    getSpv1(memberId) {
        return this.http.get(`http://healthinsight:8082/curis/qms/spv/hedis/${memberId}`).toPromise();
    }
    getMeasureInfo(measureId) {
        return this.http.get(`http://healthinsight:8082/curis/qms/work_list/${measureId}`);
    }
    createMeasure(model: any) {
        if (model.id) {
            return this.http.put(`http://healthinsight:8082/curis/qms/work_list/${model.id}`, model);
        } else {
            return this.http.post(`http://healthinsight:8082/curis/qms/work_list/`, model);
        }
       // this.http.post('http://<hostname>:<port>/curis/qms/createProgram',model);
    }
    setMeasureStatus(measureId, status,comments) {
        return this.http.put(`http://healthinsight:8082/curis/qms/work_list/status/${measureId}/${status}`, {
            "value1": comments
        });
    }
    updateCloseGaps(formModel,targetDate,closeGaps,gapId,headers){
        return this.http.post(`http://healthinsight:8082/curis/closeGaps/${closeGaps.memberId}/${gapId}`, { 'careGaps': [ {
            "measureTitle": closeGaps.careGap,
            "qualityMeasureId": gapId,
            "intervention": formModel.intervention,
            "priority": formModel.priority,
            "payerComments": formModel.payerComments,
            "providerComments": null,
            "status": formModel.status,
            "targetDate":targetDate,
            "closeGap": formModel.closeGap,
            "actionCareGap": formModel.actionOnCareGap,
            "uploadList": []
        }]},headers).pipe(
            catchError(this.handleError('Logged in user data is null. Please logout and login once.'))
          );
    }
    uploadCloseGapFiles(file){
        return this.http.post(`http://healthinsight:8082/curis/closeGaps/gic_lifecycle_import/`,file);
    }
    getMeasureCategory(id){
        return this.http.get(`http://healthinsight:8082/curis/qms/get_category_by_program_id/${id}`)
    }
    getTableName(){
     return this.http.get(`http://healthinsight:8082/curis/measure_configurator/config_data`);
    }
    getColumnName(){
      return this.http.get(`http://healthinsight:8082/curis/measure_configurator/config_data`);
      }

      getSecurityQuestions(){
      return this.http.get(`http://healthinsight:8082/curis/user/get_securityQuestions`);
      }


      getDate(event){
          return this.http.get(`http://healthinsight:8082/curis/program/program/${event}`)
      }
      getRoleList(){
          return this.http.get('http://healthinsight:8082/curis/qms/dropdown_namevalue_list/QMS_ROLE/ROLE_ID/ROLE_NAME');
      }
      getUserList(){
        return this.http.get('http://healthinsight:8082/curis/qms/dropdown_namevalue_list/QMS_USER_MASTER/USER_ID/USER_LOGINID');
      }
      getPageList(){
        return this.http.get( 'http://healthinsight:8082/curis/qms/dropdown_namevalue_list/QMS_SCREEN/SCREEN_ID/SCREEN_NAME');
      }
      getRoleData(roleId){
        return this.http.get( `http://healthinsight:8082/curis/user_role/get_role_screens/${roleId}`)
      }
      getRoleData1(roleId){
        return this.http.get( `http://healthinsight:8082/curis/user_role/get_role_screens/${roleId}`)
        .toPromise();
      }
      getStatusList(){

        return this.http.get( 'http://healthinsight:8082/curis/user_role/get_user_status_list');
      }
      createRole(roleid, array){
       //   console.log(array)
        return this.http.post(`http://healthinsight:8082/curis/user_role/add_role_screens`,{
            "roleId": roleid,
            "screenPermissions": array
            }
            )
      }
      getUserData(userId){
     return this.http.get(`http://healthinsight:8082/curis/user/get_user_by_id/${userId}`)
    }
    UserMappingSubmit(userId,roleId,status){
        return this.http.post(`http://healthinsight:8082/curis/user_role/add_user_role`, {"userId": userId,"roleId": roleId,"status": status});
    }
    getNoshowGaps()
    {
        return this.http.get(`http://healthinsight:8082/curis/qms_file/csv_output1`)
    }
    getlhcmodelsummary(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhc_modelSummary`)
    }
    getlikelihoodconfusionmatric(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/confusionMatric`)
    }
    getlikelihoodmodelScore(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/modelScore`)
    }
    getClusterStatistics(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/clusterAnalysis`)
    }
    getLikelihoodClusterStatistics(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhe_reason_not_enroll_statics`)
    }
    getClusterData(value){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/persona_cluster_features_data/${value}`);
    }
    getClusterFormData(value){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/clusterData/${value}`).toPromise();
    }
    createPersona(model,clusterId){
        return this.http.post(`http://healthinsight:8082/curis/member_engagement/update_persona`,{ "clusterId": clusterId,
        "personaName": model.personaName,
        "demographics": null,
        "motivations": model.motivations,
        "goals": model.goals,
        "barriers": model.barriers,
        "socialMedia": model.socialMedia,
        "healthStatus": model.socialMedia,          
        "demoAgeGroup":model.ageGroup,
        "demoEducation": model.education,
        "demoIncome": model.income,
        "demoOccupation":model.occupation,
        "demoAddictions":model.addiction,
        "demoFamilySize":model.familySize,
        "imageUrl":null,
        "bio":model.bio

})
    }
    getLHEModelSummary(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhe_modelSummary`)
    }
    getLHEModelMatric(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhe_modelMatric`)
    }
    getgraphdata(id,attr){
        return this.http.post(`http://healthinsight:8082/curis/member_engagement/persona_cluster_features_graph_data`,{
            "value1": id,
            "value2": attr
        });
    }
    getclusterlist(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/persona_cluster_names_list`)
    }
    getMrssList(){
        return this.http.get(`http://healthinsight:8082/curis/qms/refMrss_list`)
    }
    getMrssSampleList(){
        return this.http.get(`http://healthinsight:8082/curis/qms/refMrss_Sample_list`)
    }
    getPopulation(){
        return this.http.get(`http://healthinsight:8082/curis/pat/get_population_list`).toPromise();
    }
    getPatMeasure(){
        return this.http.get(`http://healthinsight:8082/curis/pat/get_care_gap_list`).toPromise();
    }
    getPatCodeSet(measureSK){
        return this.http.get(`http://healthinsight:8082/curis/pat/action_on_caregap_list/${measureSK}`).toPromise();
    }
    getAssociatedPatientList(measureSK,mrnIdOrName){
        return this.http.get(`http://healthinsight:8082/curis/pat/search_associated_patient_list/${measureSK}/${mrnIdOrName}`).toPromise();
    }
    createPat(model,gaps,mrn){
        return this.http.post(`http://healthinsight:8082/curis//pat//pat_create/`,{
        "measureSk":model.measure,
        "patientId":gaps.memberId,
        "compliantFlag":gaps.compliantFlag,
        "lobId":model.population,
        "mrn":mrn,
        "appointmentDate":gaps.nextAppointmentDate,
        "providerId":1,
        "gender":gaps.gender,
        "dob":gaps.dateOfBirthSk,
        "memberStatus":model.patientStatus,
        "valueSet":model.valueSet,
        "codeType":model.codeType,
        "codes":model.codes,
        "reason":model.reason         
        }).pipe(
            catchError(this.handleError(' cannot insert NULL fields please enter all the values'))
          );;
    }
    getmrnList(measuresk,query){
        return this.http.get(`http://healthinsight:8082/curis/pat/search_associated_patient_list/${measuresk}/${query}`).toPromise();
    }
    getGaps2(memberId){
        return this.http.get(`http://healthinsight:8082/curis/pat/member/${memberId}`)
    }
    uploadPatFile(model: PatScreen): Observable<PatScreen> {
        return this.http1.post(`http://healthinsight:8082/curis//pat//pat_file_import/`,model).pipe(
            catchError(this.handleError('Upload', model))
          );
    }
    getHealthyLandingData(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/role_landing_page`)
    }
    getPatHistory(memberId,measureSK){
        return this.http.get(`http://healthinsight:8082/curis/pat/get_pat/${memberId}/${measureSK}`)
    }
    getEnrollmentData(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/get_enrollment_file_output/home`);
    }

    getObjectiveEnrollment(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/get_objectives_list/Enrollments`);
    }
    getObjectiveClaims(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/get_objectives_list/Claims`);
    }
    getObjectiveMemberHealth(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/get_objectives_list/Member Health`);
    }
    getObjectiveBrandLoyalty(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/get_objectives_list/Brand Loyalty`);
    }
    getGoalsMemberDetails(id){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/getFactGoalInterventions/${id}`);
    }
    getPhysicalActivityGoal(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/dropdown_list/QMS_REF_PHYSICAL_ACTIVITY/GOAL`)
    }
    getPhysicalActivityFrequency(value){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/getQmsRefPhysicalActivityFrequency/${value}`)
    }
    getCalorieIntakeGoal(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/dropdown_list/QMS_REF_CALORIE_INTAKE/GOAL`)
    }
    getCalorieIntakeFrequency(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/dropdown_list/QMS_REF_CALORIE_INTAKE/FREQUENCY`)
    }
    getGoalsCareGap(id){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/getQmsQualityMeasureId/${id}`)
    }
    getGoalsPersonaData(id){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/get_Persona_Member_list/${id}`)
    }
    createGoals(model){
        return this.http.post(`http://healthinsight:8082/curis/enrollment/Fact_Goal_Recommendations_create`,model);
    }
    
    getModelValidationData(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/dropdown_list/QMS_REF_MODEL/MODEL_NAME`);
    }
    getCRMData(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/get_enrollment_file_output/crm`);
    }
    getVerifiedData(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/get_enrollment_file_output/verify`)
    }
    getRewardsList(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/get_all_rewards/9`)
    }
    updateRewardList(id,model){
        return this.http.post(`http://healthinsight:8082/curis/enrollment/update_rewards/${id}`,model)
    }
    updateEnrollmentrow(button,data){
        return this.http.post(`http://healthinsight:8082/curis/enrollment/update_enrollment_file_output/${button}`,data)
    }
    getPersonaNames(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/dropdown_list/PERSONA_MEMBERLIST_VIEW/PERSONA_NAME`)
    }
    getPersonaMemberList(filterType,filterValue){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/filter_Persona_Member_list/${filterType}/${filterValue}`)
    }
    getGoalsMemberList(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/getGoalRecommendationsSetMemberList`)
    }
    getRewardssMemberList(){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/getRewardRecommendationsSetMemberList`)
    }
    getGoalsRecommendationData(id){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/Fact_Goal_Recommendations_get/${id}`)
    }
    getRewardData(id){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/get_Rewards_File_Output_list/${id}`)
    }
    getSmvGeneralData(id){
        return this.http.get(`http://healthinsight:8082/curis/smv/getSMVMemberDetails/${id}`)
    }
    getSmvDataCare(id){
        return this.http.get(`http://healthinsight:8082/curis/smv/getSMVMemberPayerClustering/${id}`)
    }
    getSmvInsuranceData(id){
        return this.http.get(`http://healthinsight:8082/curis/smv/getSmvMemberClinical/${id}`)
    }
    commonfileupload(type,file){
        return this.http.post(`http://healthinsight:8082/curis/closeGaps/file_upload/${type}`,file)
    }
    getPhysicalActivityFrequencyId(goal,frequency){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/getRefPhysicalActivity/${goal}/${frequency}`)
    }
    getCalorieIntakeFrequencyId(goal,frequency){
        return this.http.get(`http://healthinsight:8082/curis/enrollment/getRefCalorieIntake/${goal}/${frequency}`)
    }
    getLikelihoodToRecommendList(){
        return this.http.get(`http://healthinsight:8082/curis/smv/getLhrMemberListView`);
    }
    getLikelihoodToClurnList(){
        return this.http.get(`http://healthinsight:8082/curis/smv/lhcMemberlistView`);
    }
    createRewards(model){
        return this.http.post(`http://healthinsight:8082/curis/enrollment/insert_Rewards_Recommendations`,model)
    }
    downloadfile(link){
        return this.http.get(`http://healthinsight:8082/curis/closeGaps/fileDownload?filePath=${link}`
        ). pipe(
        catchError(this.handleError('Upload', link)),
      )}
    createIntervention(model: Intervention): Observable<Intervention>{
        return this.http.post(`http://healthinsight:8082/curis/enrollment/getFactGoalInterventions`,model)
    }
    getGoalsRecommendationMemberList(){
        return this.http.get(`http://healthinsight:8082/curis/smv/getMemberIdList/goalsRecommendations`)
    }
    getRewardsRecommendationMemberList(){
        return this.http.get(`http://healthinsight:8082/curis/smv/getMemberIdList/rewardsRecommendations`)
    }
    getSmvData(memberId){
        return this.http.get(`http://healthinsight:8082/curis/smv/getSmvMember/${memberId}`)
        
    }
    getNcCsv(){
        return this.http.get(`http://healthinsight:8082/curis/qms_file/nc_output`)
    }
    getLheCsv(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhe_output`)
    }
    getLhcCsv(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhc_member_list`)
    }
    getnoshowModelscroe(){
        return this.http.get(`http://healthinsight:8082/curis/qms_file/csv_modelScore`)
    }
    getnoshowConfusionmetric(){
        return this.http.get(`http://healthinsight:8082/curis/qms_file/csv_confusionMatric`)
    }
    getHomepageCaregpas(){
        return this.http.get(`http://healthinsight:8082/curis/memberGapList/getHomePageCareGapsList`)
    }

    getNonComplianceOutputList(){
        return this.http.get(`http://healthinsight:8082/curis/qms_file/nc_output`)
    }
    getncModelMetric(){
        return this.http.get(`http://healthinsight:8082/curis/qms_file/nc_modelMatric`)
    }
    getncModelSummary(){
        return this.http.get(`http://healthinsight:8082/curis/qms_file/nc_modelSummary`)
    }
    getLhcModelMetric(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhc_modelMatric`)
    }
    getLheModelMetric(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhe_modelMatric`)
    }
    getpersonaClustering(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/getCPOutput`)
    }
    getpersonaClusteringstats(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/getCPStatistics`)
    }
    getpersonaClusteringfeatures(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/getCPFeature`)
    }
    getscreensforrole(id){
        return this.http.get(`http://healthinsight:8082/curis/user_role/getScreensForRole/${id}`)
    }
    getselectedpages(id){
        return this.http.get(`http://healthinsight:8082/curis/user_role/get_role_screens/${id}`)
    }
    updateQuickLink(model){
        return this.http.post(`http://healthinsight:8082/curis/user_role/updateRoleFavourites`,model)
    }
    getloblist(){
         return this.http.get(`http://healthinsight:8082/curis/qms/dropdown_namevalue_list/REF_LOB/LOB_ID/LOB`)
    }
    getproductlist(){
        return this.http.get(`http://healthinsight:8082/curis/qms/dropdown_list/DIM_PRODUCT_PLAN/LOB_PRODUCT`)
    }
}
