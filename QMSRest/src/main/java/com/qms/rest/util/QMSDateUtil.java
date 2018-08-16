package com.qms.rest.util;

import java.text.SimpleDateFormat;

public class QMSDateUtil {
	
	public static String getSQLDateFormat (java.sql.Date date) {
		if(date == null) return null;
		SimpleDateFormat simpDate = new SimpleDateFormat("dd-MMM-yy");
		return simpDate.format(date);
	}

}
