package com.qms.rest.query;

import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;

public class QueryConstant {

    public static final String DIM_MEMBEER_QUERY_ID ="SELECT dm.MEMBER_ID ,  dm.FIRST_NAME, dm.LAST_NAME  FROM DIM_MEMBER dm WHERE dm.MEMBER_ID = ?";

    public static final String DIM_MEMBEER_QUERY_NAME ="SELECT dm.MEMBER_ID ,  dm.FIRST_NAME, dm.LAST_NAME  FROM DIM_MEMBER dm WHERE dm.FIRST_NAME LIKE ? OR dm.LAST_NAME LIKE ?";
}
