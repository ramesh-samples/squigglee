// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.Match;
import com.squigglee.core.entity.Matches;
import com.squigglee.core.entity.Pattern;
import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.overlay.OverlayManager;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;
import com.squigglee.core.sketch.CountExact;
import com.squigglee.core.sketch.CountMin;
import com.squigglee.core.sketch.DyadicRange;
 
public class HandlerTests extends TestBase {
	//sample historical forex data available at http://ratedata.gaincapital.com/
	
	@org.junit.Test
	public void allOfThem() throws Exception {
		System.out.println("********************** verifyNodeUpdate **************************");
		verifyNodeUpdate();
		System.out.println("********************** verifyMultipleFrequencies **************************");
		verifyMultipleFrequencies();
		System.out.println("********************** verifySharding **************************");
		verifySharding();
		System.out.println("********************** verifyIndexAdditionDeletion **************************");
		verifyIndexAdditionDeletion();
		System.out.println("********************** verifyTimeIntervalCollapsing **************************");
		verifyTimeIntervalCollapsing();
		System.out.println("********************** verifyMasterData **************************");
		verifyMasterData();
		System.out.println("********************** verifyPatternIndexing **************************");
		verifyPatternIndexing();
		System.out.println("********************** verifyCapturedPatternNameRetrieval **************************");
		verifyCapturedPatternNameRetrieval();
		System.out.println("********************** verifyDyadicRanges **************************");
		verifyDyadicRanges();
		System.out.println("********************** verifySketchStatistics **************************");
		verifySketchStatistics();
		System.out.println("********************** verifySampledHistograms **************************");
		verifySampledHistograms();
		System.out.println("********************** verifyAddData **************************");
		verifyAddData();
		System.out.println("********************** verifyBulkData **************************");
		verifyBulkData();
		System.out.println("********************** verifyFetchData **************************");
		verifyFetchData();
		System.out.println("********************** verifyOverlayNetwork **************************");
		verifyOverlayNetwork();
		System.out.println("********************** verifySampledData **************************");
		verifySampledData();
		System.out.println("********************** verifyLocalStatus **************************");
		verifyLocalStatus();
		System.out.println("********************** verifyLimitSelect **************************");
		verifyLimitSelect();
	}
	
	@org.junit.Test
	public void verifyLimitSelect() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		IDataHandler dataHandler = HandlerFactory.getDataHandler();

		dataHandler.reset(config1.getDatatype());
		//Map<Integer,Object> data = dataHandler.fetchTimeSeries(46, "ks_data_0_int", 0, 999);
		MasterData md = HandlerFactory.getMasterDataHandler().getMasterData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), config1.getStartDate().getMillis());
		SortedMap<Long,Object> dataStart = dataHandler.fetchTimeSeriesLimit(md, 0, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, 1000, false);
		assertEquals(dataStart.firstKey(), new Long(0L));
		assertEquals(dataStart.lastKey(), new Long(999L));
		SortedMap<Long,Object> dataEnd = dataHandler.fetchTimeSeriesLimit(md, 0, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, 1000, true);
		assertEquals(dataEnd.firstKey(), new Long(4400L));
		assertEquals(dataEnd.lastKey(), new Long(5399L));		
		
		TimeSeriesConfig secondsConfig = freqConfigs.get(Frequency.SECONDS);
		MasterData md1 = HandlerFactory.getMasterDataHandler().getMasterData(secondsConfig.getCluster(), secondsConfig.getLogicalNode(), secondsConfig.getGuid(), secondsConfig.getStartDate().getMillis());
		SortedMap<Long,Object> dataSeconds = dataHandler.fetchTimeSeriesLimit(md1, 0, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, 1000, true);
		
		System.out.println(dataSeconds);
	}
	
	@org.junit.Test
	public void verifyNodeUpdate() throws TimeSeriesException {
		HandlerFactory.getMasterDataHandler().updateNode(LocalNodeProperties.getClusterName(),
				LocalNodeProperties.getNodeLogicalNumber(), 
				LocalNodeProperties.getNodeAddress(),
				LocalNodeProperties.getNodeDataCenter(),
				LocalNodeProperties.getInstanceId(),
				LocalNodeProperties.getNodeName(),
				true, true, LocalNodeProperties.isReplicaOf(),
				LocalNodeProperties.getServerStorage(),
				LocalNodeProperties.getServerType());
	}
	
	@org.junit.Test
	public void verifyMultipleFrequencies() throws Exception {
		Thread.sleep(10000);
		int[] data = new int[]{0, 1, 2, 3, 4};
		Schema.Type timeSeriesValueType = Schema.Type.INT;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		int microOffset = 899;
		int nanoOffset = 734666;
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		for (Frequency freq : freqConfigs.keySet()) {
			System.out.println("Doing test for frequency = " + freq);
			blockArray.clear();
			dataArray.clear();
			for (int j = 0; j < data.length; j++) {
				if (freq.equals(Frequency.MICROS))
					dataArray.add(handler.setDataRecord(j + microOffset, data[j]));
				else if (freq.equals(Frequency.NANOS))
					dataArray.add(handler.setDataRecord(j + nanoOffset, data[j]));
				else
					dataArray.add(handler.setDataRecord(j, data[j]));
			}
			blockArray.add(handler.setBlockRecord(freqConfigs.get(freq).getCluster(), freqConfigs.get(freq).getLogicalNode(), freqConfigs.get(freq).getGuid(), freqConfigs.get(freq).getStartDate().getMillis(), dataArray));
			GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
			byte[] serializedBytes = handler.serialize(timeSeriesRecord);
			boolean resultStatus = HandlerFactory.getDataHandler().insertBulkData(serializedBytes);
			assert resultStatus;
			Thread.sleep(20000);
			List<MasterData> mdList = HandlerFactory.getMasterDataHandler().getMasterData(freqConfigs.get(freq).getCluster(), freqConfigs.get(freq).getLogicalNode(), freqConfigs.get(freq).getGuid(), 
					freqConfigs.get(freq).getStartDate().getMillis(), freqConfigs.get(freq).getEndDate().getMillis());
			long startOffset = 0L;
			if (TimeSeriesShard.ignoreOffsets(freq))
				startOffset = (int) TimeSeriesShard.getOffset(freq, freqConfigs.get(freq).getStartDate().getMillis());
			else if (freq.equals(Frequency.MICROS))
				startOffset = (int) TimeSeriesShard.getOffset(freq, freqConfigs.get(freq).getStartDate().getMillis(), microOffset);
			else if (freq.equals(Frequency.NANOS))
				startOffset = (int) TimeSeriesShard.getOffset(freq, freqConfigs.get(freq).getStartDate().getMillis(), nanoOffset);
				
			long endOffset = startOffset + data.length - 1;			
			Map<Long,Object> storedData = HandlerFactory.getDataHandler().fetchTimeSeries(mdList.get(0), (int) startOffset, (int) endOffset);
			for ( int j = 0; j < data.length; j++) {
				if (freq.equals(Frequency.NANOS))
					assertArrayEquals(new int[]{data[j]}, new int[]{((Integer) storedData.get(startOffset + nanoOffset + j))});
				else if (freq.equals(Frequency.MICROS))
					assertArrayEquals(new int[]{data[j]}, new int[]{((Integer) storedData.get(startOffset + microOffset + j))});
				else
					assertArrayEquals(new int[]{data[j]}, new int[]{((Integer) storedData.get(startOffset + j))});
			}
			System.out.println("Success for frequency = " + freq);
		}
	}
	
	
	@org.junit.Test
	public void verifySharding() throws Exception {
		
		DateTime startDate = DateTimeHelper.parseDateString("2015-01-01T00:00:00.000Z");
		DateTime endDate = DateTimeHelper.parseDateString("2015-12-31T23:59:59.999Z");
		int millenium = (int) Math.floor(startDate.year().get() * 1.0 / 1000)*1000;
		int year = startDate.year().get();
		DateTime milleniumStartDate = new DateTime(millenium, 1,1,0,0,DateTimeHelper.UTC);
		
		Frequency freq1 = Frequency.MILLIS;
		List<Long> startTimestamps1 = TimeSeriesShard.getShardStartTimestamps(freq1, startDate, endDate);
		assertEquals(startTimestamps1.size(),365);
		assertEquals(startTimestamps1.get(0).longValue(),startDate.getMillis());
		assertEquals(startTimestamps1.get(364).longValue(),endDate.withTimeAtStartOfDay().getMillis());
		assertEquals(TimeSeriesShard.getOffset(freq1, startDate),0);
		assertEquals(TimeSeriesShard.getOffset(freq1, endDate),86399999);
		System.out.println("For millisecond data column count per row for year 2015 = " + (TimeSeriesShard.getOffset(freq1, endDate) - TimeSeriesShard.getOffset(freq1, startDate) + 1) + " per row spread across " + startTimestamps1.size() + " row(s)");
		
		Frequency freq2 = Frequency.SECONDS;
		List<Long> startTimestamps2 = TimeSeriesShard.getShardStartTimestamps(freq2, startDate, endDate);
		assertEquals(startTimestamps2.size(),1);
		assertEquals(startTimestamps2.get(0).longValue(),startDate.getMillis());
		assertEquals(TimeSeriesShard.getOffset(freq2, startDate),0);
		assertEquals(TimeSeriesShard.getOffset(freq2, endDate),31535999);
		System.out.println("For second data column count per row for year 2015 = " + (TimeSeriesShard.getOffset(freq2, endDate) - TimeSeriesShard.getOffset(freq2, startDate) + 1) + " per row spread across " + startTimestamps2.size() + " row(s)");
		
		Frequency freq3 = Frequency.MINUTES;
		List<Long> startTimestamps3 = TimeSeriesShard.getShardStartTimestamps(freq3, startDate, endDate);
		assertEquals(startTimestamps3.size(),1);
		assertEquals(startTimestamps3.get(0).longValue(),milleniumStartDate.getMillis());
		long minutes = (new Interval(milleniumStartDate,startDate)).toDuration().getStandardMinutes();
		assertEquals(TimeSeriesShard.getOffset(freq3, startDate),minutes);
		assertEquals(TimeSeriesShard.getOffset(freq3, endDate),(minutes + 60*24*365 - 1));
		System.out.println("For minute data column count per row for year 2015 = " + (TimeSeriesShard.getOffset(freq3, endDate) - TimeSeriesShard.getOffset(freq3, startDate) + 1) + " per row spread across " + startTimestamps3.size() + " row(s)");
		
		Frequency freq4 = Frequency.HOURS;
		List<Long> startTimestamps4 = TimeSeriesShard.getShardStartTimestamps(freq4, startDate, endDate);
		assertEquals(startTimestamps4.size(),1);
		assertEquals(startTimestamps4.get(0).longValue(),milleniumStartDate.getMillis());
		long hours = (new Interval(milleniumStartDate,startDate)).toDuration().getStandardHours();
		assertEquals(TimeSeriesShard.getOffset(freq4, startDate),hours);
		assertEquals(TimeSeriesShard.getOffset(freq4, endDate),(hours + 24*365 - 1));
		System.out.println("For hour data column count per row for year 2015 = " + (TimeSeriesShard.getOffset(freq4, endDate) - TimeSeriesShard.getOffset(freq4, startDate) + 1) + " per row spread across " + startTimestamps4.size() + " row(s)");
		
		Frequency freq5 = Frequency.DAYS;
		List<Long> startTimestamps5 = TimeSeriesShard.getShardStartTimestamps(freq5, startDate, endDate);
		assertEquals(startTimestamps5.size(),1);
		assertEquals(startTimestamps5.get(0).longValue(),milleniumStartDate.getMillis());
		long days = (new Interval(milleniumStartDate,startDate)).toDuration().getStandardDays();
		assertEquals(TimeSeriesShard.getOffset(freq5, startDate),days);
		assertEquals(TimeSeriesShard.getOffset(freq5, endDate),(days + 365 - 1));
		System.out.println("For day data column count per row for year 2015 = " + (TimeSeriesShard.getOffset(freq5, endDate) - TimeSeriesShard.getOffset(freq5, startDate) + 1) + " per row spread across " + startTimestamps5.size() + " row(s)");
		
		Frequency freq6 = Frequency.YEARS;
		List<Long> startTimestamps6 = TimeSeriesShard.getShardStartTimestamps(freq5, startDate, endDate);
		assertEquals(startTimestamps6.size(),1);
		assertEquals(startTimestamps6.get(0).longValue(),milleniumStartDate.getMillis());
		long years = year - millenium;
		assertEquals(TimeSeriesShard.getOffset(freq6, startDate),years);
		assertEquals(TimeSeriesShard.getOffset(freq6, endDate),years);
		System.out.println("For year data column count per row for year 2015 = " + (TimeSeriesShard.getOffset(freq6, endDate) - TimeSeriesShard.getOffset(freq6, startDate) + 1) + " per row spread across " + startTimestamps6.size() + " row(s)");


		DateTime hfStartDate = DateTimeHelper.parseDateString("2015-01-01T00:00:00.000Z");
		DateTime hfEndDate = DateTimeHelper.parseDateString("2015-01-01T00:59:59.999Z");
		Frequency freq7 = Frequency.MICROS;
		List<Long> startTimestamps7 = TimeSeriesShard.getShardStartTimestamps(freq7, hfStartDate, hfEndDate);
		assertEquals(startTimestamps7.size(),60);
		DateTime minuteStartDate = new DateTime(hfStartDate.year().get(),hfStartDate.getMonthOfYear(), hfStartDate.getDayOfMonth(),hfStartDate.getHourOfDay(), hfStartDate.getMinuteOfHour(),DateTimeHelper.UTC);
		assertEquals(startTimestamps7.get(0).longValue(),minuteStartDate.getMillis());
		DateTime minuteEndDate = new DateTime(hfEndDate.year().get(),hfEndDate.getMonthOfYear(), hfEndDate.getDayOfMonth(),hfEndDate.getHourOfDay(), hfEndDate.getMinuteOfHour(),DateTimeHelper.UTC);
		assertEquals(startTimestamps7.get(60 - 1).longValue(),minuteEndDate.getMillis());
		long offset = 350;
		long startOffset = (hfStartDate.getMillis() - minuteStartDate.getMillis())*1000 + offset;
		assertEquals(TimeSeriesShard.getOffset(freq7, minuteStartDate,offset),startOffset);
		System.out.println("For microsecond data column count per row for 1 hour = " + (60*1000*1000)  + " per row spread across " + startTimestamps7.size() + " row(s)");

		Frequency freq8 = Frequency.NANOS;
		List<Long> startTimestamps8 = TimeSeriesShard.getShardStartTimestamps(freq8, hfStartDate, hfEndDate);
		assertEquals(startTimestamps8.size(),60*60);
		DateTime secondStartDate = new DateTime(hfStartDate.year().get(),hfStartDate.getMonthOfYear(), hfStartDate.getDayOfMonth(),hfStartDate.getHourOfDay(), hfStartDate.getMinuteOfHour(),hfStartDate.getSecondOfMinute(), DateTimeHelper.UTC);
		assertEquals(startTimestamps8.get(0).longValue(),secondStartDate.getMillis());
		DateTime secondEndDate = new DateTime(hfEndDate.year().get(),hfEndDate.getMonthOfYear(), hfEndDate.getDayOfMonth(),hfEndDate.getHourOfDay(), hfEndDate.getMinuteOfHour(),hfEndDate.getSecondOfMinute(), DateTimeHelper.UTC);
		assertEquals(startTimestamps8.get(60*60 - 1).longValue(),secondEndDate.getMillis());
		offset = 350000;
		startOffset = (hfStartDate.getMillis() - secondStartDate.getMillis())*1000 + offset;
		assertEquals(TimeSeriesShard.getOffset(freq8, secondStartDate,offset),startOffset);
		System.out.println("For nanosecond data column count per row for 1 hour = " + (1000*1000*1000)  + " per row spread across " + startTimestamps8.size() + " row(s)");
	}
	
	@org.junit.Test
	public void verifyIndexAdditionDeletion() throws Exception {
		/*
		 config1 = new TimeSeriesConfig("EKG_Sample1", 0, "replication_factor:1", 
				"SimpleStrategy", 1000, DynamicTypeTranslator.getViewDataType(View.DOUBLES), "ptrn_16_1000_100_8_1000", 
				new DateTime(sdf.parse("2014-11-22T00:00:00.000-0000").getTime(),DateTimeZone.UTC), 
				new DateTime(sdf.parse("2014-11-22T23:59:59.999-0000").getTime(),DateTimeZone.UTC));
		
		config2 = new TimeSeriesConfig("ZIPF_Sample1", 0, "replication_factor:1", 
				"SimpleStrategy", 1000, DynamicTypeTranslator.getViewDataType(View.INTEGERS), "skchCM_59999_8192_50_1000", 
				new DateTime(sdf.parse("2014-11-22T00:00:00.000-0000").getTime(),DateTimeZone.UTC), 
				new DateTime(sdf.parse("2014-11-22T23:59:59.999-0000").getTime(),DateTimeZone.UTC)); 
		 */
		
		List<TimeSeriesConfig> list = HandlerFactory.getMasterDataHandler().getMasterData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid());
		String oldIndex = list.get(0).getIndexes();
		assertEquals(oldIndex, config1.getIndexes());
		String newIndex = "sometestIndex_10_100";
		HandlerFactory.getIndexHandler().updateIndex(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), newIndex, false);
		list = HandlerFactory.getMasterDataHandler().getMasterData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid());
		assertEquals(oldIndex + ";" + newIndex, list.get(0).getIndexes());
		HandlerFactory.getIndexHandler().updateIndex(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), newIndex, true);
		assertEquals(oldIndex, config1.getIndexes());
	}
	
	@org.junit.Test
	public void verifyTimeIntervalCollapsing() throws Exception {
		List<TimeSeriesConfig> rawList = new ArrayList<TimeSeriesConfig>();
		rawList.add(new TimeSeriesConfig(config1.getCluster(), "Test", 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.DOUBLES), "ptrn_16_1000_100_8_1000", 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z")));
		
		rawList.add(new TimeSeriesConfig(config1.getCluster(), "Test", 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.DOUBLES), "ptrn_16_1000_100_8_1000", 
				DateTimeHelper.parseDateString("2014-11-23T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-23T23:59:59.999Z")));

		rawList.add(new TimeSeriesConfig(config1.getCluster(), "Test", 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.DOUBLES), "ptrn_16_1000_100_8_1000", 
				DateTimeHelper.parseDateString("2014-11-24T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-24T23:59:59.999Z")));
		
		rawList.add(new TimeSeriesConfig(config1.getCluster(), "Test", 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.DOUBLES), "ptrn_16_1000_100_8_1000", 
				DateTimeHelper.parseDateString("2014-11-27T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-27T23:59:59.999Z")));

		rawList.add(new TimeSeriesConfig(config1.getCluster(), "Test", 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.DOUBLES), "ptrn_16_1000_100_8_1000", 
				DateTimeHelper.parseDateString("2014-11-28T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-28T23:59:59.999Z")));
		HandlerFactory.getMasterDataHandler().getNodeMasterData(config1.getCluster(), 0);
		List<TimeSeriesConfig> collapsedList = TimeSeriesConfig.collapseTimeIntervals(rawList);
		assertEquals(2, collapsedList.size());
		assertEquals(collapsedList.get(1).getStartDate().getMillis(),(DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z")).getMillis());
		assertEquals(collapsedList.get(1).getEndDate().getMillis(),(DateTimeHelper.parseDateString("2014-11-24T23:59:59.999Z")).getMillis());
		assertEquals(collapsedList.get(0).getStartDate().getMillis(),(DateTimeHelper.parseDateString("2014-11-27T00:00:00.000Z")).getMillis());
		assertEquals(collapsedList.get(0).getEndDate().getMillis(),(DateTimeHelper.parseDateString("2014-11-28T23:59:59.999Z")).getMillis());
		
	}
	
	@org.junit.Test
	public void verifyMasterData() throws Exception {
		List<MasterData> mdList = HandlerFactory.getMasterDataHandler().getMasterData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), 
				config1.getStartDate().getMillis(), config1.getEndDate().getMillis());
		assertEquals(1,mdList.size());
		assertEquals(0,mdList.get(0).getId());
		assertEquals(config1.getGuid(), mdList.get(0).getGuid());
		assertEquals(config1.getIndexes(),mdList.get(0).getIndexes());
		assertEquals(config1.getLogicalNode(), mdList.get(0).getLn());
		assertEquals(config1.getDatatype(), mdList.get(0).getDatatype());
		assertEquals(config1.getFrequency(), mdList.get(0).getFreq());
		assertEquals(config1.getStartDate().getMillis(), mdList.get(0).getStartts());
	}
	
	@org.junit.Test
	public void verifyPatternIndexing() throws Exception {
		Thread.sleep(20000); //sleep just a bit to let the pattern indexes complete their jobs
		
		List<Double> data = getSampleEKGData(); // already stored 
		String pguid = java.util.UUID.randomUUID().toString();
		int startIndex = 103;
		//the 11 through 26 values in the test data set 
		Double[] pattern = new Double[16];
		double[] ptrn = new double[16];
		List<String> patternList = new ArrayList<String>();
		for (int i=0; i<pattern.length; i++) {
			pattern[i] = data.get(startIndex + i);
			ptrn[i] = data.get(startIndex + i);
			patternList.add(new Double(data.get(startIndex + i)).toString());
		}
		//for (int i = 0; i < pattern.length; i++)
		HandlerFactory.getPatternHandler().storePattern(config1.getCluster(), pguid, patternList);
		
		List<String> fetchedPatternList = HandlerFactory.getPatternHandler().fetchPattern(config1.getCluster(), pguid);
		Double[] fetchedPattern = new Double[pattern.length];
		for (int i=0; i< fetchedPatternList.size(); i++)
			fetchedPattern[i] = Double.valueOf(fetchedPatternList.get(i));
		assertArrayEquals(pattern,fetchedPattern);
		List<TimeSeriesConfig> idList = new ArrayList<TimeSeriesConfig>();
		idList.add(config1);
		double radius = 2.0;
		int topk = 3;
		
		Matches request = new Matches();
		request.setPattern(new Pattern(patternList));
		request.setRadius(radius);
		request.setTopk(topk);
		request.setRequestDomain(idList);
		HandlerFactory.getPatternHandler().processMatches(request);
		
		for (double dist : request.getMatchResults().keySet()) {
			for (Match m : request.getMatchResults().get(dist)) {
				System.out.println("Found match at offset = " + m.getValues().firstKey() + " at distance = " + m.getDistance() 
					+ " from cluster = " + m.getCluster() + " and ln = " + m.getLn() + " for id = " + m.getId());
			}
		}
		//System.in.read();
	}
	
	@org.junit.Test
	public void verifyCapturedPatternNameRetrieval() throws Exception {

		List<Double> data = getSampleEKGData(); // already stored 
		String pguid = java.util.UUID.randomUUID().toString();
		int startIndex = 35;
		//the 11 through 26 values in the test data set 
		Double[] pattern = new Double[16];
		double[] ptrn = new double[16];
		List<String> patternList = new ArrayList<String>();
		for (int i=0; i<pattern.length; i++) {
			pattern[i] = data.get(startIndex + i);
			ptrn[i] = data.get(startIndex + i);
			patternList.add(new Double(data.get(startIndex + i)).toString());
		}
		
		//for (int i = 0; i < pattern.length; i++)
		HandlerFactory.getPatternHandler().storePattern(config1.getCluster(), pguid, patternList);
		
		List<String> fetchedPatternList = HandlerFactory.getPatternHandler().fetchCapturedPatterns(config1.getCluster());
		assert(fetchedPatternList.contains(pguid));
		for (String fetchedpguid : fetchedPatternList)
			System.out.println(fetchedpguid);
		//System.in.read();
	}	

	@org.junit.Test
	public void verifyDyadicRanges() throws Exception {
		Thread.sleep(10000);
		// e.g. for n = 64 and query range [12,27]
		// covering ranges are -- 8:[17,24],4:[13,16],1:[12,12],2:[25,26],1:[27,27]		
		List<DyadicRange> ranges = DyadicRange.getCoveringRanges(64, 12, 27);
		for (DyadicRange range : ranges) {
			System.out.println(range.getOrder() + ";" + range.getStart() + ";" + range.getEnd() + ";" + range.getOriginalStart() + ";" + range.getOriginalEnd());
		}
		
		// e.g. for n = 1000000 and query range [1,100000]
		ranges = DyadicRange.getCoveringRanges(1000000, 1, 1000000);
		for (DyadicRange range : ranges) {
			System.out.println(range.getOrder() + ";" + range.getStart() + ";" + range.getEnd() + ";" + range.getOriginalStart() + ";" + range.getOriginalEnd());
		}
		
		System.in.read();
	}
	
	@org.junit.Test
	public void verifySketchStatistics() throws Exception {
		//Thread.sleep(10000);
		List<MasterData> list = HandlerFactory.getMasterDataHandler().getMasterData(config2.getCluster(), config2.getLogicalNode(), config2.getGuid(), 
				config2.getStartDate().getMillis(), config2.getEndDate().getMillis());
		assert list.size() == 1;
		Map<Long,Object> dataMap = HandlerFactory.getDataHandler().fetchTimeSeries(list.get(0), 0, 999);
		Map<Integer,Long> actualDistribution = new HashMap<Integer,Long>();
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;
		double first = 0, last = 0;
		long count = 0;
		for (Object i : dataMap.keySet()) {
			Integer val = (Integer) dataMap.get(i);
			double dval = Double.parseDouble(val.toString());
			if (dval <= min)
				min = dval;
			if (dval >= max)
				max = dval;
			if (count == 0)
				first = dval;
			last = dval;
			count++;
			if (actualDistribution.containsKey(val))
				actualDistribution.put(val, (actualDistribution.get(val).longValue() + 1));
			else
				actualDistribution.put(val, new Long(1));
		}
		
		CountMin cmSketch = new CountMin(3,1000000,32768,200,1,10, -1, -1);
		for (Object data : dataMap.keySet()) {
			int value = Integer.parseInt(dataMap.get(data).toString());
			cmSketch.update(((Long) data).intValue(), value);
		}
		
		Stats stats = cmSketch.statistics();
		System.out.println("Count Min Sketch Statistics");
		System.out.println("Sketch max = " + stats.getMax());
		System.out.println("Sketch min = " + stats.getMin());
		System.out.println("Sketch first = " + stats.getFirst());
		System.out.println("Sketch last = " + stats.getLast());
		System.out.println("Sketch count = " + stats.getCount());
		System.out.println("Heavy Hitters = " + stats.getHeavyHitters());
		System.out.println("Actual Distribution = " + actualDistribution);
		assertEquals(new Double(min), new Double(stats.getMin()));
		assertEquals(new Double(max), new Double(stats.getMax()));
		assertEquals(new Double(first), new Double(stats.getFirst()));
		assertEquals(new Double(last), new Double(stats.getLast()));
		assertEquals(new Long(count), new Long(stats.getCount()));
		for (Long topkcount : stats.getHeavyHitters().keySet()) {
			Integer val = new Integer((int) ( (Double) stats.getHeavyHitters().get(topkcount)).doubleValue());
			assertEquals(topkcount,actualDistribution.get(val));
		}
		for (Object o : actualDistribution.keySet())
			assertTrue(cmSketch.pointQuery(Double.parseDouble(o.toString())) == actualDistribution.get(o));
		assertTrue(count == cmSketch.rangeQuery(min, max));

		CountExact exactSketch = new CountExact(3,1000000,1,10, -1, -1);
		for (Object data : dataMap.keySet()) {
			int value = Integer.parseInt(dataMap.get(data).toString());
			exactSketch.update(((Long) data).intValue(), value);
		}
		Stats estats = exactSketch.statistics();
		System.out.println("Exact Sketch Statistics");
		System.out.println("Sketch max = " + estats.getMax());
		System.out.println("Sketch min = " + estats.getMin());
		System.out.println("Sketch first = " + estats.getFirst());
		System.out.println("Sketch last = " + estats.getLast());
		System.out.println("Sketch count = " + estats.getCount());
		System.out.println("Heavy Hitters = " + estats.getHeavyHitters());
		System.out.println("Actual Distribution = " + actualDistribution);
		
		assertEquals(new Double(min), new Double(estats.getMin()));
		assertEquals(new Double(max), new Double(estats.getMax()));
		assertEquals(new Double(first), new Double(estats.getFirst()));
		assertEquals(new Double(last), new Double(estats.getLast()));
		assertEquals(new Long(count), new Long(estats.getCount()));
		for (Long topkcount : estats.getHeavyHitters().keySet()) {
			Integer val = new Integer((int) ( (Double) estats.getHeavyHitters().get(topkcount)).doubleValue());
			assertEquals(topkcount, actualDistribution.get(val));
		}
		for (Object o : actualDistribution.keySet())
			assertTrue(exactSketch.pointQuery(Double.parseDouble(o.toString())) == actualDistribution.get(o));
		assertTrue(count == exactSketch.rangeQuery(min, max));
		
		HandlerFactory.getSketchHandler().reset("int");
		for (int quantile = 0; quantile <=100; quantile++) {
			Object result = HandlerFactory.getSketchHandler().inverseQuery(config2.getCluster(), config2.getLogicalNode(), config2.getGuid(), quantile/100.0);
			System.out.println(quantile + "," + result);
		}
		System.out.println("Done with inverse query");
	}
	
	@org.junit.Test
	public void verifySampledHistograms() throws Exception {
		SortedMap<Integer,Long> sketchHistogram = HandlerFactory.getSketchHandler().getSketchHistogram(config2.getCluster(), 0,config2.getGuid(),10);
		System.out.println("Sketch Histogram");
		System.out.println(sketchHistogram);
		//DateTime start = new DateTime(sdf.parse("2015-01-21T00:00:00.000-0000").getTime(),DateTimeZone.UTC);
		//DateTime end = new DateTime(sdf.parse("2015-01-21T00:00:59.999-0000").getTime(),DateTimeZone.UTC);
		SortedMap<Integer,Long>  sampledDataHistogram = HandlerFactory.getSampledDataHandler().getSampledDataHistogram(config2.getCluster(), 0, config2.getGuid(), 100, 
				config2.getStartDate().getMillis(), 0, config2.getStartDate().getMillis() + 999, 0, 100, HandlerFactory.getSketchHandler());
		
		System.out.println("Sampled Data Histogram subsample size 10000");
		System.out.println(sampledDataHistogram);
	}
	
	@org.junit.Test
	public void verifyAddData() throws Exception {
		//Thread.sleep(10000);
		List<MasterData> mdList = HandlerFactory.getMasterDataHandler().getMasterData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), 
				config1.getStartDate().getMillis(), config1.getEndDate().getMillis());
		
		double[] data = new double[]{1.1, 2.2, 3.3, 4.4, 5.5};
		Schema.Type timeSeriesValueType = Schema.Type.DOUBLE;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		for (int j = 0; j < data.length; j++)
			dataArray.add(handler.setDataRecord(j, data[j]));
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		blockArray.add(handler.setBlockRecord(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), config1.getStartDate().getMillis(), dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		boolean resultStatus = HandlerFactory.getDataHandler().insertBulkData(serializedBytes);
		assert resultStatus;
		Thread.sleep(20000);
		Map<Long,Object> storedData = HandlerFactory.getDataHandler().fetchTimeSeries(mdList.get(0), 0, data.length-1);
		for ( Long j = 0L; j < data.length; j++)
			assertArrayEquals(new Double[]{data[j.intValue()]}, new Double[]{((Double) storedData.get(j))});
	}
	
	@org.junit.Test
	public void verifyBulkData() throws Exception {
		Thread.sleep(10000);
		//List<MasterData> mdList = mdHandler.getMasterData(config1.getLogicalNode(), config1.getGuid(), 
		//		config1.getStartDate().getMillis(), config1.getEndDate().getMillis());
		List<Double> data = new ArrayList<Double>();
		data.add(1.1);data.add(2.2);data.add(3.3);data.add(4.4);data.add(5.5);
		//double[] data = new double[]{1.1, 2.2, 3.3, 4.4, 5.5};
		Schema.Type timeSeriesValueType = Schema.Type.DOUBLE;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		for (int j = 0; j < data.size(); j++)
			dataArray.add(handler.setDataRecord(j, data.get(j)));
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		blockArray.add(handler.setBlockRecord(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), config1.getStartDate().getMillis(), dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		boolean resultStatus = HandlerFactory.getDataHandler().insertBulkData(serializedBytes);
		assert resultStatus;
		Thread.sleep(20000);
		byte[] deserializedBytes = (byte []) (HandlerFactory.getDataHandler().readBlockData(config1.getCluster(), 0, config1.getGuid(), 
				config1.getStartDate().getMillis(), 0, config1.getStartDate().getMillis() + 4, 0))[2];
		/*
		AvroTimeSeriesDeserializer atsd = new AvroTimeSeriesDeserializer();
		atsd.setRawData(deserializedBytes);
		int numOfBlocks = atsd.getBlockCount();
		int cnt = 0;
		assertEquals(1, numOfBlocks);
		for (int i=0; i< numOfBlocks; i++) {
			int dataCount = atsd.getDataCount(i);
			for (int j=0; j< dataCount; j++) {	
				++cnt;
				assertTrue(data.contains((Double) atsd.getVal(i, j)));
			}
		}
		assertEquals(5,cnt);
		*/
		
		assertArrayEquals(serializedBytes, deserializedBytes);
	}
	
	@org.junit.Test
	public void verifyFetchData() throws Exception {
		
		double[] data = new double[]{1.1, 2.2, 3.3, 4.4, 5.5};
		Schema.Type timeSeriesValueType = Schema.Type.DOUBLE;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		for (int j = 0; j < data.length; j++)
			dataArray.add(handler.setDataRecord(j, data[j]));
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		blockArray.add(handler.setBlockRecord(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), config1.getStartDate().getMillis(), dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		boolean resultStatus = HandlerFactory.getDataHandler().insertBulkData(serializedBytes);
		assert resultStatus;
		Thread.sleep(20000);
		MasterData md = HandlerFactory.getMasterDataHandler().getMasterData(config1.getCluster(), 0, config1.getGuid(), config1.getStartDate().getMillis());
		Map<Long, Object> readResults = HandlerFactory.getDataHandler().fetchTimeSeries(md, 0, 4);
		
		for (int i = 0; i< readResults.size(); i++)
			assert(data[i] == Double.parseDouble(readResults.get(i).toString()));
		
	}
	
	@org.junit.Test
	public void verifyOverlayNetwork() throws Exception {
		
		Map<String,Map<String, Map<Integer, List<String>>>> overlayNetwork = HandlerFactory.getMasterDataHandler().getOverlayNetwork();
		assertEquals(overlayNetwork.keySet().size(), 2);
		
		for (TimeSeriesConfig config : new TimeSeriesConfig[]{config1, config2}) {
			assertTrue(overlayNetwork.containsKey(config.getDatatype()));
			assertTrue(overlayNetwork.get(config.getDatatype()).containsKey(config.getCluster()));
			assertTrue(overlayNetwork.get(config.getDatatype()).get(config.getCluster()).containsKey(config.getLogicalNode()));
			assertTrue(overlayNetwork.get(config.getDatatype()).get(config.getCluster()).get(config.getLogicalNode()).contains(config.getGuid()));
		}
		
		String vdbContents = OverlayManager.buildOverlayVdb(overlayNetwork);
		System.out.println(vdbContents);
	}

	@org.junit.Test
	public void verifySampledData() throws Exception {
		Double[] data = new Double[]{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.10};
		Schema.Type timeSeriesValueType = Schema.Type.DOUBLE;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		for (int j = 0; j < data.length; j++)
			dataArray.add(handler.setDataRecord(j, data[j]));
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		blockArray.add(handler.setBlockRecord(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), 
				config1.getStartDate().getMillis(), dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		boolean resultStatus = HandlerFactory.getDataHandler().insertBulkData(serializedBytes);
		assert resultStatus;
		Thread.sleep(10000);
		//List<Double> dataList = Arrays.asList(data);
		
		TimeSeries ts = HandlerFactory.getSampledDataHandler().readBlockData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), 
				config1.getStartDate().getMillis(), 0, config1.getStartDate().getMillis() + 9, 0, 5);
		assertEquals(5,ts.getData().size());
	}

	@org.junit.Test
	public void verifyLocalStatus() throws Exception {
		//List<NodeStatus> clStatus = statusHandler.fetchClusterStatus();
		//System.out.println(clStatus.get(0).isStorageServiceUp());
	}

	@org.junit.Test
	public void verifyPatternIndexing2() throws Exception {
		
		
		System.in.read(); //sleep just a bit to let the pattern indexes complete their jobs
		
		List<Integer> data = getSampleZipfData(); // already stored 
		
		
		for (int i=103; i<(103+24); i++)
			System.out.print(data.get(i) + ",");
		System.out.println("Values starting at index = " + 103);
		for (int i=36648; i<(36648+24); i++)
			System.out.print(data.get(i) + ",");
		System.out.println("Values starting at index = " + 36648);
		for (int i=56313; i<(56313+24); i++)
			System.out.print(data.get(i) + ",");
		System.out.println("Values starting at index = " + 56313);
		
		
		String pguid = java.util.UUID.randomUUID().toString();
		int startIndex = 103; 
		Double[] pattern = new Double[24];
		double[] ptrn = new double[24];
		List<String> patternList = new ArrayList<String>();
		for (int i=0; i<pattern.length; i++) {
			pattern[i] = data.get(startIndex + i).doubleValue();
			ptrn[i] = data.get(startIndex + i);
			patternList.add(new Double(data.get(startIndex + i)).toString());
		}
		//for (int i = 0; i < pattern.length; i++)
		HandlerFactory.getPatternHandler().storePattern(config2.getCluster(), pguid, patternList);
		
		List<String> fetchedPatternList = HandlerFactory.getPatternHandler().fetchPattern(config2.getCluster(), pguid);
		Double[] fetchedPattern = new Double[pattern.length];
		for (int i=0; i< fetchedPatternList.size(); i++)
			fetchedPattern[i] = Double.valueOf(fetchedPatternList.get(i));
		assertArrayEquals(pattern,fetchedPattern);
		List<TimeSeriesConfig> idList = new ArrayList<TimeSeriesConfig>();
		idList.add(config2);
		
		//System.in.read();
		double radius = 2.0;
		int topk = 3;
		Matches request = new Matches();
		request.setPattern(new Pattern(patternList));
		request.setRadius(radius);
		request.setTopk(topk);
		request.setRequestDomain(idList);
		HandlerFactory.getPatternHandler().processMatches(request);
		
		for (double dist : request.getMatchResults().keySet()) {
			for (Match m : request.getMatchResults().get(dist)) {
				System.out.println("Found match at offset = " + m.getValues().firstKey() + " at distance = " + m.getDistance() 
					+ " from cluster = " + m.getCluster() + " and ln = " + m.getLn() + " for id = " + m.getId());
			}
		}
		//System.in.read();
	}

}