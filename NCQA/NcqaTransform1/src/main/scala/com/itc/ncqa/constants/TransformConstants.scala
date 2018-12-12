package com.itc.ncqa.constants

object TransformConstants {


    var sourcedbName = ""
    val generalMembershipTableName = "general_membership"


    var targetDbName = ""
    val dimMemberTableName = "dim_member"
    val dimDateTableName = "dim_date"

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
