package com.qms.rest.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class QMSDateUtil {
	
	public static String getSQLDateFormat (java.sql.Date date) {
		if(date == null) return null;
		SimpleDateFormat simpDate = new SimpleDateFormat("dd-MMM-yy");
		return simpDate.format(date);
	}
	
	public static long getDateInLong (String dateStr, String dateFormat) {
		if(dateFormat == null) dateFormat =  "dd-MMM-yy";
		if(dateStr == null) return 0;
		SimpleDateFormat simpDate = new SimpleDateFormat(dateFormat);
		long date =  0;
		try {
			date = simpDate.parse(dateStr).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
	
	public static String getPhoenixDate(String date) {
		try {
		    return getDate(date, "dd-MMM-yy", "yyyy-MM-dd");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}	
	
	public static String getDate(String date, String currentFormat, String expectedFormat) 
			throws ParseException {
		if (date == null || currentFormat == null || expectedFormat == null) {
			return null;
		}
		SimpleDateFormat sourceDateFormat = new SimpleDateFormat(currentFormat);
		Date dateObj = sourceDateFormat.parse(date);
		SimpleDateFormat desiredDateFormat = new SimpleDateFormat(expectedFormat);
		return desiredDateFormat.format(dateObj).toString();
	}	

}
