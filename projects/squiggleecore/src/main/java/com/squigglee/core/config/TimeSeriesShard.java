// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.serializers.DynamicTypeTranslator;

public class TimeSeriesShard {

	public static List<Long> getShardStartTimestamps(Frequency freq, long startts, long endts) {
		return getShardStartTimestamps(freq, new DateTime(startts), new DateTime(endts));
	}
	
	public static List<Long> getShardStartTimestamps(Frequency freq, DateTime start, DateTime end) {
		DateTime startDate = start.toDateTime(DateTimeHelper.UTC).withTimeAtStartOfDay();
		DateTime endDate = end.toDateTime(DateTimeHelper.UTC);
		int year = startDate.year().get();
		DateTime startYear = new DateTime(year,1,1,0,0,DateTimeHelper.UTC);
		int millenium = (int) Math.floor(startDate.year().get() * 1.0 / 1000)*1000;
		DateTime milleniumStartYear = new DateTime(millenium,1,1,0,0,DateTimeHelper.UTC);
		List<Long> list = new ArrayList<Long>();
		switch (freq) {
			case NANOS: {
				//for microsecond data store one second worth of data in one row i.e the data offset is from the start of the minute for the data timestamp 
				DateTime startSecondDate = new DateTime(startDate.getYear(), startDate.getMonthOfYear(), startDate.getDayOfMonth(), 
						startDate.getHourOfDay(),startDate.getMinuteOfHour(), startDate.getSecondOfMinute(), DateTimeHelper.UTC);
				
				for (DateTime date = startSecondDate; date.isBefore(endDate); date = date.plusSeconds(1)) {
					long ts = date.getMillis();
					if (!list.contains(ts))
						list.add(ts);
				}
				break;
			}
			case MICROS: {
				//for microsecond data store one minute worth of data in one row i.e the data offset is from the start of the minute for the data timestamp 
				DateTime startMinuteDate = new DateTime(startDate.getYear(), startDate.getMonthOfYear(), startDate.getDayOfMonth(), 
						startDate.getHourOfDay(),startDate.getMinuteOfHour(),DateTimeHelper.UTC);
				
				for (DateTime date = startMinuteDate; date.isBefore(endDate); date = date.plusMinutes(1)) {
					long ts = date.getMillis();
					if (!list.contains(ts))
						list.add(ts);
				}
				break;
			}
			case MILLIS: {
				//for millisecond data store one day worth of data in one row i.e the data offset is from the start of the day for the data timestamp 
				for (DateTime date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
					long ts = date.getMillis();
					if (!list.contains(ts))
						list.add(ts);
				}
				break;
			}
			case YEARS: case DAYS: case HOURS:	case MINUTES: {
				//for these store an entire millenium's worth of data in a single row i.e the data offset is from the start of the millenium for the data timestamp 
				for (DateTime date = milleniumStartYear; date.isBefore(endDate); date = date.plusYears(1000)) {
					long ts = date.getMillis();
					if (!list.contains(ts))
						list.add(ts);
				}
				break;
			}
			case SECONDS: {
				//for second data store one years worth of data in a single row i.e the data offset is from the start of the year for the data timestamp 
				for (DateTime date = startYear; date.isBefore(endDate); date = date.plusYears(1)) {
					long ts = date.getMillis();
					if (!list.contains(ts))
						list.add(ts);
				}
				break;
			}
			default:
				break;
		}
		return list;
	}
	
	public static long getOffset(Frequency freq, DateTime timestamp) {
		return getOffset(freq, timestamp, 0);
	}
	
	public static long getOffset(Frequency freq, DateTime timestamp, long hfOffset){

		DateTime  ts = timestamp.toDateTime(DateTimeHelper.UTC);
		DateTime startDay = ts.withTimeAtStartOfDay();
		int year = ts.year().get();
		DateTime startYear = new DateTime(year,1,1,0,0,DateTimeHelper.UTC);
		int millenium = (int) Math.floor(ts.year().get() * 1.0 / 1000)*1000;
		DateTime milleniumStartYear = new DateTime(millenium,1,1,0,0,DateTimeHelper.UTC);
		switch (freq) {
			case NANOS: {
				hfOffset = hfOffset % 1000000;	// simply drop any offset part greater than a million, will overwrite, no exceptions thrown 
				//for nano-second data user provides one offset from possible additional billion offsets   
				DateTime startSecondDate = new DateTime(ts.getYear(), ts.getMonthOfYear(), ts.getDayOfMonth(), ts.getHourOfDay(),ts.getMinuteOfHour(),
						ts.getSecondOfMinute(), DateTimeHelper.UTC);
				return ((ts.getMillis() - startSecondDate.getMillis())*1000000 + hfOffset);
			}
			case MICROS: {
				hfOffset = hfOffset % 1000;	// simply drop any offset part greater than a thousand, will overwrite, no exceptions thrown 
				//for micro-second data user provides one offset from possible additional million offsets   
				DateTime startMinuteDate = new DateTime(ts.getYear(), ts.getMonthOfYear(), ts.getDayOfMonth(), ts.getHourOfDay(),ts.getMinuteOfHour(),
						DateTimeHelper.UTC);
				return ((ts.getMillis() - startMinuteDate.getMillis())*1000 + hfOffset);
			}
			case MILLIS: {
				//hfOffset ignored
				//for millisecond data offsets range from [0,86399999] 
				//add 1 or 2 more offsets for special days with leap seconds added for UTC
				return (ts.getMillis() - startDay.getMillis());
			}
			case YEARS: {
				//hfOffset ignored
				//for yearly data offsets range from [0,] (non-leap year) or [0,] (leap-year) with a few extra for leap second cases
				return (year - millenium);
			}
			case DAYS: {
				//hfOffset ignored
				//for hourly data offsets range from [0,] (non-leap year) or [0,] (leap-year) with a few extra for leap second cases
				long days = (new Interval(milleniumStartYear,ts)).toDuration().getStandardDays();
				return days;
			}
			case HOURS: {
				//hfOffset ignored
				//for hourly data offsets range from [0,] (non-leap year) or [0,] (leap-year) with a few extra for leap second cases
				long hours = (new Interval(milleniumStartYear,ts)).toDuration().getStandardHours();
				return hours;
			}
			case MINUTES: {
				//hfOffset ignored
				//for minutes data offsets range from [0,525599999] (non-leap year) or [0,527039999] (leap-year) with a few extra for leap second cases
				long minutes = (new Interval(milleniumStartYear,ts)).toDuration().getStandardMinutes();
				return minutes;
			}
			case SECONDS: {
				//hfOffset ignored
				//for seconds data offsets range from [0,31535999] (non-leap year) or [0,31622399] (leap-year) with a few extra for leap second cases  
				long seconds = (new Interval(startYear,ts)).toDuration().getStandardSeconds();
				return seconds;	
			}
			default:
				break;
		}
		return -1;
	}

	public static long getOffsetCount(MasterData md) {
		return getOffsetCount(md.getStartts(), md.getFreq());
	}
	
	public static long getOffsetCount(long startts, Frequency freq) {
		DateTime mdStartDate = new DateTime(startts, DateTimeHelper.UTC);
		DateTime maxEndDate = getMaxEndDate(freq, mdStartDate);
		if (freq.equals(Frequency.MICROS))
			return 60000000;
		else if (freq.equals(Frequency.NANOS))
			return 1000000000;
		else
			return TimeSeriesShard.getOffset(freq, maxEndDate) + 1;
	}
	
	public static DateTime getMaxEndDate(Frequency freq, DateTime startTimestamp) {
		switch (freq) {
		case NANOS:
			return startTimestamp.plusSeconds(1).minusMillis(1); 	// 1000 milliseconds * 1000000 offsets per row
		case MICROS:
			return startTimestamp.plusMinutes(1).minusMillis(1);	// 60,000 milliseconds * 1000 offsets per row  
		case DAYS:
			return startTimestamp.plusYears(1000).minusDays(1);	// one whole millennium per row
		case HOURS:
			return startTimestamp.plusYears(1000).minusHours(1);	// one whole millennium per row
		case MILLIS:
			return startTimestamp.plusDays(1).minusMillis(1); // one full day per row 
		case MINUTES:
			return startTimestamp.plusYears(1000).minusMinutes(1);	// one whole millennium per row
		case SECONDS:
			return startTimestamp.plusYears(1).minusSeconds(1);	// one whole year per row
		case YEARS:
			return startTimestamp.plusYears(1000).minusYears(1);	// one whole millennium per row
		default:
			break;
		}
		return null;
	}

	public static DateTime getMaxEndDate(Frequency freq, long ts) {
		return getMaxEndDate(freq, new DateTime(ts, DateTimeHelper.UTC));
	}
	
	public static long getOffset(Frequency freq, long ts) {
		return getOffset(freq, new DateTime(ts));
	}
	
	public static long getOffset(Frequency freq, long ts, long hfOffset) {
		return getOffset(freq, new DateTime(ts), hfOffset);
	}
	
	public static DateTime advance(Frequency freq, long ts, int steps) {
		DateTime timestamp = new DateTime(ts,DateTimeHelper.UTC);
		return advance(freq,timestamp,steps);
	}
	
	public static DateTime advance(Frequency freq, DateTime timestamp, int steps) {
		switch (freq) {
		case NANOS:
			return timestamp.plusSeconds(steps);
		case MICROS:
			return timestamp.plusMinutes(steps);
		case DAYS:
			return timestamp.plusDays(steps);
		case HOURS:
			return timestamp.plusHours(steps);	
		case MILLIS:
			return timestamp.plusMillis(steps);  
		case MINUTES:
			return timestamp.plusMinutes(steps);	
		case SECONDS:
			return timestamp.plusSeconds(steps);	
		case YEARS:
			return timestamp.plusYears(steps);	
		default:
			break;
		}
		return null;
	}
	
	public static DateTime retrace(Frequency freq, long ts, int steps) {
		DateTime timestamp = new DateTime(ts,DateTimeHelper.UTC);
		return retrace(freq,timestamp,steps);
	}
	
	public static DateTime retrace(Frequency freq, DateTime timestamp, int steps) {
		switch (freq) {
		case NANOS:
			return timestamp.minusSeconds(steps);
		case MICROS:
			return timestamp.minusMinutes(steps);
		case DAYS:
			return timestamp.minusDays(steps);
		case HOURS:
			return timestamp.minusHours(steps);	
		case MILLIS:
			return timestamp.minusMillis(steps);  
		case MINUTES:
			return timestamp.minusMinutes(steps);	
		case SECONDS:
			return timestamp.minusSeconds(steps);	
		case YEARS:
			return timestamp.minusYears(steps);	
		default:
			break;
		}
		return null;
	}

	public static boolean ignoreOffsets(Frequency freq) {
		switch (freq) {
		case DAYS:
			return true;
		case HOURS:
			return true;
		case MICROS:
			return false;
		case MILLIS:
			return true;
		case MINUTES:
			return true;
		case NANOS:
			return false;
		case SECONDS:
			return true;
		case YEARS:
			return true;
		default:
			break;
		}
		return false;
	}

	public static long getRolledUpTime(long startts, int offset, Frequency from, Frequency to) {
		long rolledUpTs = startts;
		switch(from) {
		case NANOS:
			break;
		case MICROS:
			break;
		case DAYS:
		case HOURS:
		case MILLIS:
		case MINUTES:
		case SECONDS:
		case YEARS:
			DateTime actualTime = advance(from, startts, offset);
			rolledUpTs = getRolledUpTime(actualTime, to);
			break;
		default:
			break; 
		}
		return rolledUpTs;
	}
	
	public static long getRolledUpTime(DateTime actualTime, Frequency to) {
		DateTime rolledUpTime = actualTime;
			switch (to) {
			case NANOS:
				break;
			case MICROS:
				break;
			case DAYS:
				rolledUpTime = new DateTime(actualTime.getYear(), actualTime.getMonthOfYear(), actualTime.getDayOfMonth(), 
						0, 0, 0, 0, DateTimeZone.UTC);
				break;
			case HOURS:
				rolledUpTime = new DateTime(actualTime.getYear(), actualTime.getMonthOfYear(), actualTime.getDayOfMonth(), 
						actualTime.getHourOfDay(), 0, 0, 0, DateTimeZone.UTC);
				break;	
			case MILLIS:
				rolledUpTime = new DateTime(actualTime.getYear(), actualTime.getMonthOfYear(), actualTime.getDayOfMonth(), 
						actualTime.getHourOfDay(), actualTime.getMinuteOfHour(), actualTime.getSecondOfMinute(),
						actualTime.getMillisOfSecond(), DateTimeZone.UTC); 
				break;
			case MINUTES:
				rolledUpTime = new DateTime(actualTime.getYear(), actualTime.getMonthOfYear(), actualTime.getDayOfMonth(), 
						actualTime.getHourOfDay(), actualTime.getMinuteOfHour(), 0,	0, DateTimeZone.UTC);
				break;	
			case SECONDS:
				rolledUpTime = new DateTime(actualTime.getYear(), actualTime.getMonthOfYear(), actualTime.getDayOfMonth(), 
						actualTime.getHourOfDay(), actualTime.getMinuteOfHour(), actualTime.getSecondOfMinute(), 0, DateTimeZone.UTC);
				break;
			case YEARS:
				rolledUpTime = new DateTime(actualTime.getYear(), 1, 1, 
						0, 0, 0, 0, DateTimeZone.UTC);
				break;	
			default:
				break;
			}
			return rolledUpTime.getMillis();
	}
	
	public static int getRolledUpOffset(int hfOffset, Frequency to) {
		int rolledUpOffset = hfOffset;
		switch (to) {
		case NANOS:
			break;
		case MICROS:
			rolledUpOffset = hfOffset / 1000;
			break;
		case DAYS:
		case HOURS:	
		case MILLIS:
		case MINUTES:
		case SECONDS:
		case YEARS:
			break;	
		default:
			break;
		}
		return rolledUpOffset;
	}
	
	public static Map<Frequency,Map<Long, SortedMap<Long, Object>>> getRollups(SortedMap<Long, Object> data, MasterData md) throws TimeSeriesException {
		Map<Frequency,Map<Long, SortedMap<Long, Object>>> rollups = new HashMap<Frequency,Map<Long, SortedMap<Long, Object>>>();
		if (data == null || data.isEmpty())
			return rollups;
		for (Frequency rolledUpFreq : getRollupFrequencies(md.getFreq())) {
			rollups.put(rolledUpFreq, getRollUp(data, md, rolledUpFreq));
		}
		return rollups;
	}
	
	public static Map<Long, SortedMap<Long, Object>> getRollUp(SortedMap<Long, Object> data, MasterData md, Frequency rolledUpFreq) throws TimeSeriesException {
		Map<Long, SortedMap<Long, Object>> rollup = new HashMap<Long, SortedMap<Long, Object>>();
		double sum = 0.0;
		int count = 0;
		long currentRolledUpTime = TimeSeriesShard.getRolledUpTime(md.getStartts(), data.firstKey().intValue(), md.getFreq(), rolledUpFreq);
		long start = TimeSeriesShard.advance(md.getFreq(), md.getStartts(), data.firstKey().intValue()).getMillis();
		long end = TimeSeriesShard.advance(md.getFreq(), md.getStartts(), data.lastKey().intValue()).getMillis();
		List<Long> rolledUpStartTimestamps = TimeSeriesShard.getShardStartTimestamps(rolledUpFreq, start, end);
		for (Long offset : data.keySet()) { 
			long actualTime = TimeSeriesShard.advance(md.getFreq(), md.getStartts(), offset.intValue()).getMillis();
			long rolledUpTime = TimeSeriesShard.getRolledUpTime(md.getStartts(), offset.intValue(), md.getFreq(), rolledUpFreq);
			long rolledUpStartts = 0L;
			for (long startts : rolledUpStartTimestamps) {
				if (actualTime >= startts) {
					rolledUpStartts = startts;
					break;
				}
			}
			if (!rollup.containsKey(rolledUpStartts))
				rollup.put(rolledUpStartts, new TreeMap<Long, Object>());
			long rolledUpOffset = TimeSeriesShard.getOffset(rolledUpFreq, rolledUpTime);
			if (rolledUpTime == currentRolledUpTime) {
				sum += Double.parseDouble(data.get(offset).toString());
				count++;
			} else {
				rollup.get(rolledUpStartts).put(rolledUpOffset, DynamicTypeTranslator.convertDoubleToNumeric(new Double(sum / count), md.getDatatype()));
				currentRolledUpTime = rolledUpTime;
				sum = 0.0;
				count = 0;
			}
			if (offset.equals(data.lastKey()) && count > 0)
				rollup.get(rolledUpStartts).put(rolledUpOffset, DynamicTypeTranslator.convertDoubleToNumeric(new Double(sum / count), md.getDatatype()));
		}
		return rollup;
	}
	
	public static Map<Frequency,Map<Long, Set<Long>>> getRollups(long startts, long endts, MasterData md) throws TimeSeriesException {
		Map<Frequency,Map<Long, Set<Long>>> rollups = new HashMap<Frequency,Map<Long, Set<Long>>>();
		for (Frequency rolledUpFreq : getRollupFrequencies(md.getFreq())) {
			rollups.put(rolledUpFreq, getRollUp(startts, endts, md, rolledUpFreq));
		}
		return rollups;
	}
	
	public static Map<Long, Set<Long>> getRollUp(long startts, long endts, MasterData md, Frequency rolledUpFreq) throws TimeSeriesException {
		Map<Long, Set<Long>> rollup = new HashMap<Long, Set<Long>>();
		int count = 0;
		long currentRolledUpTime = TimeSeriesShard.getRolledUpTime(md.getStartts(), (int) startts, md.getFreq(), rolledUpFreq);
		//long start = TimeSeriesShard.advance(md.getFreq(), md.getStartts(), (int) startts).getMillis();
		//long end = TimeSeriesShard.advance(md.getFreq(), md.getStartts(), (int) endts).getMillis();
		List<Long> rolledUpStartTimestamps = TimeSeriesShard.getShardStartTimestamps(rolledUpFreq, startts, endts);
		long rolledUpOffsetMin = Long.MAX_VALUE, rolledUpOffsetMax = Long.MIN_VALUE;
		for (long offset = startts; offset <= endts; offset++) { 
			long actualTime = TimeSeriesShard.advance(md.getFreq(), md.getStartts(), (int) offset).getMillis();
			long rolledUpTime = TimeSeriesShard.getRolledUpTime(md.getStartts(), (int) offset, md.getFreq(), rolledUpFreq);
			long rolledUpStartts = 0L;
			for (long rs : rolledUpStartTimestamps) {
				if (actualTime >= rs) {
					rolledUpStartts = rs;
					break;
				}
			}
			if (!rollup.containsKey(rolledUpStartts))
				rollup.put(rolledUpStartts, new TreeSet<Long>());
			long rolledUpOffset = TimeSeriesShard.getOffset(rolledUpFreq, rolledUpTime);
			if (rolledUpOffset > rolledUpOffsetMax)
				rolledUpOffsetMax = rolledUpOffset;
			if (rolledUpOffset < rolledUpOffsetMin)
				rolledUpOffsetMin = rolledUpOffset;
			
			if (rolledUpTime != currentRolledUpTime) {
				rollup.get(rolledUpStartts).add(rolledUpOffsetMin);
				rollup.get(rolledUpStartts).add(rolledUpOffsetMax);
				currentRolledUpTime = rolledUpTime;
				count = 0;
				rolledUpOffsetMin = Long.MAX_VALUE;
				rolledUpOffsetMax = Long.MIN_VALUE;
			} else 
				count++;
			if (offset == endts && count > 0) {
				rollup.get(rolledUpStartts).add(rolledUpOffsetMin);
				rollup.get(rolledUpStartts).add(rolledUpOffsetMax);
			}
		}
		return rollup;
	}
	
	public static List<Frequency> getRollupFrequencies(Frequency rawFrequency) {
		switch(rawFrequency) {
		case DAYS:
			return Arrays.asList(new Frequency[]{Frequency.YEARS});
		case HOURS:
			return Arrays.asList(new Frequency[]{Frequency.YEARS, Frequency.DAYS});
		case MICROS:
			return Arrays.asList(new Frequency[]{Frequency.YEARS, Frequency.DAYS, Frequency.HOURS, Frequency.MINUTES, 
					Frequency.SECONDS, Frequency.MILLIS});
		case MILLIS:
			return Arrays.asList(new Frequency[]{Frequency.YEARS, Frequency.DAYS, Frequency.HOURS, Frequency.MINUTES, 
					Frequency.SECONDS});
		case MINUTES:
			return Arrays.asList(new Frequency[]{Frequency.YEARS, Frequency.DAYS, Frequency.HOURS});
		case NANOS:
			return Arrays.asList(new Frequency[]{Frequency.YEARS, Frequency.DAYS, Frequency.HOURS, Frequency.MINUTES, 
					Frequency.SECONDS, Frequency.MILLIS, Frequency.MICROS});
		case SECONDS:
			return Arrays.asList(new Frequency[]{Frequency.YEARS, Frequency.DAYS, Frequency.HOURS, Frequency.MINUTES});
		case YEARS:
			break;
		default:
			break;
		}
		return new ArrayList<Frequency>();
	}
}
