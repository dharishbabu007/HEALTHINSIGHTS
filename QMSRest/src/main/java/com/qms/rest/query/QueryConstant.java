package com.qms.rest.query;

import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;

public class QueryConstant {

    public static final String DIM_MEMBEER_QUERY ="SELECT dm.MEMBER_ID ,  dm.FIRST_NAME, dm.LAST_NAME  FROM DIM_MEMBER dm WHERE dm.MEMBER_ID = ? OR dm.FIRST_NAME = ?";
}
