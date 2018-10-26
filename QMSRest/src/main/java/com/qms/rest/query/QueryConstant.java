package com.qms.rest.query;

public class QueryConstant {

    public static final String DIM_MEMBEER_QUERY_ID ="SELECT dm.MEMBER_ID, dm.FIRST_NAME, dm.MIDDLE_NAME, dm.LAST_NAME FROM DIM_MEMBER dm WHERE dm.MEMBER_ID LIKE ?";

    public static final String DIM_MEMBEER_QUERY_NAME ="SELECT dm.MEMBER_ID, dm.FIRST_NAME, dm.MIDDLE_NAME, dm.LAST_NAME FROM DIM_MEMBER dm WHERE lower(dm.FIRST_NAME) LIKE ? OR lower(dm.MIDDLE_NAME) LIKE ? OR lower(dm.LAST_NAME) LIKE ?";
}
