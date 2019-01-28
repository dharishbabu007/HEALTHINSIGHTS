import { CachedHttpClient } from './cache-httpclient';
import { Injectable } from '@angular/core';
import { environment } from '../../../environments/environment';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { HttpErrorHandler, HandleError } from '../../shared/services/http-error-handler.service';

@Injectable()
export class GapsService {
    private handleError: HandleError;
    constructor(private http: CachedHttpClient,  httpErrorHandler: HttpErrorHandler,) {
        this.handleError =  httpErrorHandler.createHandleError('GapsService');
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
    getLikelihoodMeasureDetails() {
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhe_output`);
    }
    getCommunicationMeasureDetails(){
        return this.http.get('');
    }
    getLikehoodchurnMeasureDetails(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/lhe_output`);    
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
        }]},headers);
    }
    uploadCloseGapFiles(file){
        return this.http.post(`http://healthinsight:8082/curis/closeGaps/gic_lifecycle_import/`,file). pipe(
            catchError(this.handleError('Upload', file))
          );
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
          return this.http.get(`http://healthinsight:8082/curis/qms/program/${event}`)
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
        return this.http.post(`http://healthinsight:8082/curis/user_role/add_role_screens`,{
            "roleId": roleid,
            "screenPermissions": [
                                            {
                                            "screenId": array[0].screenid,
                                            "read":array[0].read,
                                            "write":array[0].write,
                                            "download": array[0].download
                                            }
                            ]
            }
            );
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
    getlikelihoodmodel(){
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/modelSummary`)
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
        return this.http.get(`http://healthinsight:8082/curis/member_engagement/clusterData/${value}`);
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
}
