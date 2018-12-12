package com.itc.ncqa.constants

object TransformConstants {


    var sourcedbName = ""
    val generalMembershipTableName = "general_membership"
    val providerTableName = "provider"
    val visitTableName = "visit"
    val labTableName = "lab"
    val membershipEnrollTableName = "membership_enrollment"
    val observationTableName = "observation"
    val pharmacyClinicalTableName = "pharmacy_clinical"
    val pharmacyTableName = "Pharmacy"


    var targetDbName = ""
    val dimMemberTableName = "dim_member"
    val dimProviderTableName = "dim_provider"
    val dimDateTableName = "dim_date"
    val factClaimsTableName = "fact_claims"
    val factLabsTableName = "fact_labs"
    val factMembershipTablename = "fact_membership"
    val factObservationTableName = "fact_observation"
    val factPharmacyClinTableName = "fact_pharmacy_clinical"
    val factRxClaimsTableName = "fact_rx_claims"
    val refLobTableName = "ref_lob"


    /*set the source db name*/
    def setSourceDbName(sourceDbVal:String):String = {
      sourcedbName = sourceDbVal
      sourcedbName
    }

    /*set the target db name*/
    def setTargetDbName(targetDbVal:String):String = {
    targetDbName = targetDbVal
    targetDbName
    }
}
