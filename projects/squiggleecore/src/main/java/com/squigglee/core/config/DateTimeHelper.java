// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.s
package com.squigglee.core.config;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
//import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class DateTimeHelper {

	//protected static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	protected static DateTimeFormatter dtf = ISODateTimeFormat.dateTime();
	public static DateTimeZone UTC = DateTimeZone.UTC;
	
	public static String getDateString(DateTime dt) {
		return dtf.withZoneUTC().print(dt);
	}
	
	public static DateTime parseDateString(String dateString) {
		//return DateTimeFormat.forPattern(dateFormat).withZoneUTC().parseDateTime(dateString);
		return dtf.withZoneUTC().parseDateTime(dateString);
	}

	public static String getSampleStartOfToday() {
		return getDateString(DateTime.now(UTC).withTimeAtStartOfDay());
	}
	
	public static String getSampleEndOfToday() {
		return getDateString(new DateTime((DateTime.now(UTC).plusDays(1).withTimeAtStartOfDay()).getMillis()  - 1));
	}
	
	public static String getNow() {
		return dtf.withZoneUTC().print(DateTime.now());
	}
	
	public static long getNowStartts() {
		return DateTime.now(UTC).getMillis();
	}
}
