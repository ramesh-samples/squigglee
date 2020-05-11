// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.Match;
import com.squigglee.core.entity.Matches;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.Operators;
import com.squigglee.core.entity.Pattern;
import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;
import com.squigglee.core.sketch.CountExact;
import com.squigglee.core.sketch.CountMin;
import com.squigglee.core.sketch.DyadicRange;
 
public class RestTests extends TestBaseRest {
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
		System.out.println("********************** verifySampledData **************************");
		verifySampledData();
		System.out.println("********************** verifyLimitSelect **************************");
		verifyLimitSelect();
	}
	
	@org.junit.Test
	public void verifyLimitSelect() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//HandlerFactory factory = new HandlerFactory();
		//IDataHandler dataHandler =factory.getNewDataHandler();

		//dataHandler.reset(config1.getDatatype());
		//Map<Integer,Object> data = dataHandler.fetchTimeSeries(46, "ks_data_0_int", 0, 999);
		//MasterData md = mdHandler.getMasterData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), config1.getStartDate().getMillis());
		//MasterData md = mdHandler.getMasterData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), config1.getStartDate().getMillis());
		
		SortedMap<Long,Object> dataStart = RESTFactory.getTimeSeriesProxy(addr).getSequencedTimeSeriesJSON(config1.getCluster(), config1.getLogicalNode(), 
				config1.getGuid(), config1.getStartDate().getMillis(), 0, config1.getEndDate().getMillis(), 0, 1000, false).getData();
		
		//SortedMap<Long,Object> dataStart = dataHandler.fetchTimeSeriesLimit(md, 0, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, 1000, false);
		assertEquals(dataStart.firstKey(), new Long(0L));
		assertEquals(dataStart.lastKey(), new Long(999L));
		
		SortedMap<Long,Object> dataEnd = RESTFactory.getTimeSeriesProxy(addr).getSequencedTimeSeriesJSON(config1.getCluster(), config1.getLogicalNode(), 
				config1.getGuid(), config1.getStartDate().getMillis(), 0, config1.getEndDate().getMillis(), 0, 1000, true).getData();
		//SortedMap<Long,Object> dataEnd = dataHandler.fetchTimeSeriesLimit(md, 0, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, 1000, true);
		assertEquals(dataEnd.firstKey(), new Long(4400L));
		assertEquals(dataEnd.lastKey(), new Long(5399L));		
		
		TimeSeriesConfig secondsConfig = freqConfigs.get(Frequency.SECONDS);
		//MasterData md1 = mdHandler.getMasterData(secondsConfig.getCluster(), secondsConfig.getLogicalNode(), secondsConfig.getGuid(), secondsConfig.getStartDate().getMillis());
		
		//SortedMap<Long,Object> dataSeconds = dataHandler.fetchTimeSeriesLimit(md1, 0, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, 1000, true);
		SortedMap<Long,Object> dataSeconds = RESTFactory.getTimeSeriesProxy(addr).getSequencedTimeSeriesJSON(secondsConfig.getCluster(), secondsConfig.getLogicalNode(), 
				secondsConfig.getGuid(), secondsConfig.getStartDate().getMillis(), 0, secondsConfig.getEndDate().getMillis(), 0, 1000, true).getData();
		
		assertEquals(dataSeconds.size(), 5);
		System.out.println(dataSeconds);
	}
	
	@org.junit.Test
	public void verifyNodeUpdate() throws TimeSeriesException {		
		RESTFactory.getConfigurationProxy(addr).updateNode(LocalNodeProperties.getClusterName(), LocalNodeProperties.getNodeLogicalNumber(), 
				LocalNodeProperties.getNodeAddress(),
				LocalNodeProperties.getNodeDataCenter(),
				LocalNodeProperties.getInstanceId(),
				LocalNodeProperties.getNodeName(),
				LocalNodeProperties.isBoostrapNode(), 
				LocalNodeProperties.isSeedNode(), 
				LocalNodeProperties.isReplicaOf(),
				LocalNodeProperties.getServerStorage(),
				LocalNodeProperties.getServerType());
		
		Map<String, List<NodeStatus>> map = RESTFactory.getStatusProxy(addr).fetchGlobalStatus();
		assertEquals(map.size(), 1);
		assertTrue(map.containsKey(LocalNodeProperties.getClusterName()));
		List<NodeStatus> list = RESTFactory.getStatusProxy(addr).fetchClusterStatus(LocalNodeProperties.getClusterName());
		assertEquals(list.size(), 1);
		assertTrue(list.get(0).getAddress().equalsIgnoreCase(LocalNodeProperties.getNodeAddress()));
		
		NodeStatus ns = RESTFactory.getStatusProxy(addr).fetchNodeStatus(LocalNodeProperties.getClusterName(), LocalNodeProperties.getNodeLogicalNumber());
		assertEquals(ns.getCluster(),LocalNodeProperties.getClusterName());
		assertEquals(ns.getLogicalNumber(),LocalNodeProperties.getNodeLogicalNumber());
		assertEquals(ns.getAddress(),LocalNodeProperties.getNodeAddress());
		assertEquals(ns.getDataCenter(),LocalNodeProperties.getNodeDataCenter());
		assertEquals(ns.getInstanceId(),LocalNodeProperties.getInstanceId());
		assertEquals(ns.getName(),LocalNodeProperties.getNodeName());
		assertEquals(ns.isBootstrapNode(),LocalNodeProperties.isBoostrapNode());
		assertEquals(ns.isSeedNode(),LocalNodeProperties.isSeedNode());
		assertEquals(ns.getReplicaOf(),LocalNodeProperties.isReplicaOf());
		assertEquals(ns.getStorage(),LocalNodeProperties.getServerStorage());
		assertEquals(ns.getStype(),LocalNodeProperties.getServerType());
		// only when services & jobs are running
		//assertTrue(ns.isNodeUp());
		//assertTrue(ns.isOverlayUp());
	}
	
	@org.junit.Test
	public void verifyMultipleFrequencies() throws Exception {
		//Thread.sleep(10000);
		int[] data = new int[]{0, 1, 2, 3, 4};
		Schema.Type timeSeriesValueType = Schema.Type.INT;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		int microOffset = 899;
		int nanoOffset = 734666;
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		for (Frequency freq : freqConfigs.keySet()) {
			//if (!freq.equals(Frequency.SECONDS))
			//	continue;
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
			TimeSeries ts = new TimeSeries();
			ts.setCluster(freqConfigs.get(freq).getCluster());
			ts.setLn(freqConfigs.get(freq).getLogicalNode());
			ts.setId(freqConfigs.get(freq).getGuid());
				
			ts.setBulkData(RESTFactory.encode(serializedBytes));
			//IDataHandler dataHandler = (new HandlerFactory()).getNewDataHandler();
			//TimeSeriesProxy tsProxy = new TimeSeriesProxy(dataHandler, freqConfigs.get(freq).getLogicalNode(), freqConfigs.get(freq).getCluster(), 10000);
			
			//tsProxy.putData(freqConfigs.get(freq).getCluster(), freqConfigs.get(freq).getLogicalNode(), freqConfigs.get(freq).getGuid(), 
			//		freqConfigs.get(freq).getStartDate().getMillis(), microOffset, freqConfigs.get(freq).getEndDate().getMillis(), 
			//		(microOffset + data.length -1), restFactory.encode(serializedBytes));
			
			TimeSeries result = RESTFactory.getTimeSeriesProxy(addr).updateTimeSeriesBulkJSON(ts);
			boolean resultStatus = (result.getErrorMessage() == null)?true:false;
			assertTrue(resultStatus);
			Thread.sleep(20000);
			long startOffset = (int) TimeSeriesShard.getOffset(freq, freqConfigs.get(freq).getStartDate().getMillis());
			result.setStart(freqConfigs.get(freq).getStartDate().getMillis());
			result.setEnd(freqConfigs.get(freq).getEndDate().getMillis());
			Map<Long,Object> storedData = null;
			if (freq.equals(Frequency.NANOS)) {
				result.setStartHfOffset(nanoOffset);
				result.setEndHfOffset(nanoOffset + data.length - 1);
				storedData = RESTFactory.getTimeSeriesProxy(addr).getTimeSeriesJSON(result).getData();
				//storedData = dataHandler.fetchTimeSeries(mdList.get(0), (int) startOffset + nanoOffset, 0, (int) endOffset + nanoOffset, 0);
			}
			else if (freq.equals(Frequency.MICROS)) {
				result.setStartHfOffset(microOffset);
				result.setEndHfOffset(microOffset + data.length - 1);
				storedData = RESTFactory.getTimeSeriesProxy(addr).getTimeSeriesJSON(result).getData();
				//storedData = dataHandler.fetchTimeSeries(mdList.get(0), (int) startOffset + microOffset, 0, (int) endOffset + microOffset, 0);
			}
			else {
				result.setStartHfOffset(0);
				result.setEndHfOffset(0);
				storedData = RESTFactory.getTimeSeriesProxy(addr).getTimeSeriesJSON(result).getData();
				//storedData = dataHandler.fetchTimeSeries(mdList.get(0), (int) startOffset, 0, (int) endOffset, 0);
			}
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
		
		//List<TimeSeriesConfig> list = mdHandler.getMasterData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid());
		List<TimeSeriesConfig> list = RESTFactory.getConfigurationProxy(addr).getConfigJSON(config1.getCluster(), config1.getLogicalNode(), config1.getGuid());
		String oldIndex = list.get(0).getIndexes();
		assertEquals(oldIndex, config1.getIndexes());
		String newIndex = "sometestIndex_10_100";
		//idxHandler.updateIndex(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), newIndex, false);
		TimeSeriesConfig request = config1.clone();
		request.setIndexes(newIndex);
		RESTFactory.getConfigurationProxy(addr).addIndexJSON(request);
		//list = mdHandler.getMasterData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid());
		list = RESTFactory.getConfigurationProxy(addr).getConfigJSON(config1.getCluster(), config1.getLogicalNode(), config1.getGuid());
		assertEquals(oldIndex + ";" + newIndex, list.get(0).getIndexes());
		RESTFactory.getConfigurationProxy(addr).dropIndexJSON(request);
		//idxHandler.updateIndex(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), newIndex, true);
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
		//mdHandler.getNodeMasterData(config1.getCluster(), 0);
		RESTFactory.getConfigurationProxy(addr).getConfigJSON(config1.getCluster(), 0);
		List<TimeSeriesConfig> collapsedList = TimeSeriesConfig.collapseTimeIntervals(rawList);
		assertEquals(2, collapsedList.size());
		assertEquals(collapsedList.get(1).getStartDate().getMillis(),(DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z")).getMillis());
		assertEquals(collapsedList.get(1).getEndDate().getMillis(),(DateTimeHelper.parseDateString("2014-11-24T23:59:59.999Z")).getMillis());
		assertEquals(collapsedList.get(0).getStartDate().getMillis(),(DateTimeHelper.parseDateString("2014-11-27T00:00:00.000Z")).getMillis());
		assertEquals(collapsedList.get(0).getEndDate().getMillis(),(DateTimeHelper.parseDateString("2014-11-28T23:59:59.999Z")).getMillis());
		
	}
	
	@org.junit.Test
	public void verifyMasterData() throws Exception {
		
		List<TimeSeriesConfig> mdList = RESTFactory.getConfigurationProxy(addr).getConfigJSON(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), 
				config1.getStartDate().getMillis(), config1.getEndDate().getMillis());
		
		assertEquals(1,mdList.size());
		assertEquals(config1.getGuid(), mdList.get(0).getGuid());
		assertEquals(config1.getIndexes(),mdList.get(0).getIndexes());
		assertEquals(config1.getLogicalNode(), mdList.get(0).getLogicalNode());
		assertEquals(config1.getDatatype(), mdList.get(0).getDatatype());
		assertEquals(config1.getFrequency(), mdList.get(0).getFrequency());
		assertTrue(config1.getStartDate().getMillis() >=  mdList.get(0).getStartDate().getMillis());
	}
	
	@org.junit.Test
	public void verifyPatternIndexing() throws Exception {
		//System.in.read();
		//Thread.sleep(20000); //sleep just a bit to let the pattern indexes complete their jobs
		
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
			//ptrnHandler.storePattern(config1.getCluster(), pguid, patternList);
		
		Pattern patternObj = new Pattern(config1.getCluster(), pguid, patternList);
		
		RESTFactory.getPatternProxy(addr).storePatternJSON(patternObj);
		assertTrue(patternObj.getErrorMessage() == null);
		
		patternObj.setVals(null);
		patternObj = RESTFactory.getPatternProxy(addr).fetchStoredPatternJSON(config1.getCluster(), pguid);
		

		Double[] fetchedPattern = new Double[pattern.length];
		for (int i=0; i< patternObj.getVals().size(); i++)
			fetchedPattern[i] = Double.valueOf(patternObj.getVals().get(i));
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
		request.setDataLocal(true);
		Matches result = RESTFactory.getPatternProxy(addr).fetchMatchesJSON(request);
		//ptrnHandler.processMatches(request);
		
		for (double dist : result.getMatchResults().keySet()) {
			for (Match m : result.getMatchResults().get(dist)) {
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
		
		Pattern patternObj = new Pattern(config1.getCluster(), pguid, patternList);
		
		RESTFactory.getPatternProxy(addr).storePatternJSON(patternObj);
		patternObj.setVals(null);
		//List<String> fetchedPatternList = ptrnHandler.fetchCapturedPatterns(config1.getCluster());
		//patternObj = patternRestClient.fetchStoredPatternJSON(patternObj);
		
		//for (String fetchedpguid : fetchedPatternList)
		//	System.out.println(fetchedpguid);
		//System.in.read();
	}	

	@org.junit.Test
	public void verifyDyadicRanges() throws Exception {
		//Thread.sleep(10000);
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
		
		//System.in.read();
	}
	
	@org.junit.Test
	public void verifySketchStatistics() throws Exception {
		
		TimeSeries ts = new TimeSeries();
		ts.setCluster(config2.getCluster());
		ts.setLn(config2.getLogicalNode());
		ts.setId(config2.getGuid());
		ts.setStart(config2.getStartDate().getMillis());
		ts.setEnd(config2.getStartDate().getMillis() + 999 );
		Map<Long,Object> dataMap = RESTFactory.getTimeSeriesProxy(addr).getTimeSeriesJSON(ts).getData();
		
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
		
		//sketchHandler.reset("int");
		for (int quantile = 0; quantile <=100; quantile++) {
			Object result = RESTFactory.getSynopsesProxy(addr).inverseQuery(config2.getCluster(), config2.getLogicalNode(), config2.getGuid(), "" + quantile/100.0);
			//Object result = sketchHandler.inverseQuery(config2.getCluster(), config2.getLogicalNode(), config2.getGuid(), quantile/100.0);
			System.out.println(quantile + "," + result);
		}
		System.out.println("Done with inverse query");
	}
	
	@org.junit.Test
	public void verifySampledHistograms() throws Exception {
		SortedMap<Integer,Long> sketchHistogram = RESTFactory.getSynopsesProxy(addr).getSketchHistogram(config2.getCluster(), config2.getLogicalNode(),config2.getGuid(),10);
		System.out.println("Sketch Histogram");
		System.out.println(sketchHistogram);
		
		SortedMap<Integer,Long>  sampledDataHistogram = RESTFactory.getSynopsesProxy(addr).getSampledDataHistogram(config2.getCluster(), config2.getLogicalNode(), 
				config2.getGuid(), config2.getStartDate().getMillis(), 0, config2.getEndDate().getMillis(), 0, 100, 10000);
		System.out.println("Sampled Data Histogram subsample size 10000");
		System.out.println(sampledDataHistogram);
	}
	
	@org.junit.Test
	public void verifyAddData() throws Exception {
		
		//ITimeSeriesRESTService client = (ITimeSeriesRESTService) restFactory.getProxy("127.0.0.1", ITimeSeriesRESTService.class);
		
		double[] data = new double[]{1.1, 2.2, 3.3, 4.4, 5.5};
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
		
		TimeSeries ts = new TimeSeries();
		ts.setCluster(config1.getCluster());
		ts.setLn(config1.getLogicalNode());
		ts.setId(config1.getGuid());
		ts.setStart(config1.getStartDate().getMillis());
		ts.setEnd(config1.getStartDate().getMillis() + data.length - 1 );
		ts.setBulkData(RESTFactory.encode(serializedBytes));

		TimeSeries updateResult = RESTFactory.getTimeSeriesProxy(addr).updateTimeSeriesBulkJSON(ts);
		if (updateResult.getErrorMessage() != null)
			System.out.println(updateResult.getErrorMessage());
		
		Thread.sleep(10000);
		ts.setBulkData(null);
		TimeSeries fetchResult = RESTFactory.getTimeSeriesProxy(addr).getTimeSeriesJSON(ts);

		Map<Long,Object> storedData = fetchResult.getData();
		for ( Long j = 0L; j < data.length; j++)
			assertArrayEquals(new Double[]{data[j.intValue()]}, new Double[]{((Double) storedData.get(j))});
	}
	
	@org.junit.Test
	public void verifyBulkData() throws Exception {
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
		
		TimeSeries ts = new TimeSeries();
		ts.setCluster(config1.getCluster());
		ts.setLn(config1.getLogicalNode());
		ts.setId(config1.getGuid());
		ts.setStart(config1.getStartDate().getMillis());
		ts.setEnd(config1.getStartDate().getMillis() + data.size() - 1 );
		ts.setBulkData(RESTFactory.encode(serializedBytes));
		TimeSeries result = RESTFactory.getTimeSeriesProxy(addr).updateTimeSeriesBulkJSON(ts);
		boolean resultStatus = (result.getErrorMessage() == null)?true:false;
		//boolean resultStatus = dataHandler.insertBulkData(serializedBytes);
		assert resultStatus;
		Thread.sleep(10000);
		
		TimeSeries fetched = RESTFactory.getTimeSeriesProxy(addr).getTimeSeriesBulkJSON(result);
		byte[] deserializedBytes = RESTFactory.decode(fetched.getBulkData());
		//byte[] deserializedBytes = (byte []) (dataHandler.readBlockData(config1.getCluster(), 0, config1.getGuid(), 
		//		config1.getStartDate().getMillis(), 0, config1.getStartDate().getMillis() + 4, 0))[2];
		
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
		//GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		//byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		TimeSeries ts = new TimeSeries();
		ts.setCluster(config1.getCluster());
		ts.setLn(config1.getLogicalNode());
		ts.setId(config1.getGuid());
		ts.setStart(config1.getStartDate().getMillis());
		ts.setEnd(config1.getStartDate().getMillis() + data.length - 1 );
		TimeSeries result = RESTFactory.getTimeSeriesProxy(addr).updateTimeSeriesBulkJSON(ts);
		boolean resultStatus = (result.getErrorMessage() == null)?true:false;
		//boolean resultStatus = dataHandler.insertBulkData(serializedBytes);
		assert resultStatus;
		Thread.sleep(10000);
		//MasterData md = mdHandler.getMasterData(config1.getCluster(), 0, config1.getGuid(), config1.getStartDate().getMillis());
		Map<Long, Object> readResults = RESTFactory.getTimeSeriesProxy(addr).getTimeSeriesJSON(ts).getData();
		//Map<Long, Object> readResults = dataHandler.fetchTimeSeries(md, 0, 0, 4, 0);
		
		for (int i = 0; i< readResults.size(); i++)
			assert(data[i] == Double.parseDouble(readResults.get(i).toString()));
		
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
		TimeSeries ts = new TimeSeries();
		ts.setCluster(config1.getCluster());
		ts.setLn(config1.getLogicalNode());
		ts.setId(config1.getGuid());
		ts.setStart(config1.getStartDate().getMillis());
		ts.setEnd(config1.getStartDate().getMillis() + data.length - 1 );
		ts.setBulkData(RESTFactory.encode(serializedBytes));
		TimeSeries result = RESTFactory.getTimeSeriesProxy(addr).updateTimeSeriesBulkJSON(ts);
		boolean resultStatus = (result.getErrorMessage() == null)?true:false;
		//boolean resultStatus = dataHandler.insertBulkData(serializedBytes);
		assert resultStatus;
		
		TimeSeries sampledData = RESTFactory.getSynopsesProxy(addr).getSampledTimeSeriesJSON(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), 
				config1.getStartDate().getMillis(), 0 , config1.getStartDate().getMillis() + 9, 0, 5);
		assertEquals(5,sampledData.getData().size());
	}

	@org.junit.Test
	public void verifyPatternIndexing2() throws Exception {
		
		
		//System.in.read(); //sleep just a bit to let the pattern indexes complete their jobs
		
		List<Integer> data = getSampleZipfData(); // already stored 
		
		/*
		for (int i=103; i<(103+24); i++)
			System.out.print(data.get(i) + ",");
		System.out.println("Values starting at index = " + 103);
		for (int i=36648; i<(36648+24); i++)
			System.out.print(data.get(i) + ",");
		System.out.println("Values starting at index = " + 36648);
		for (int i=56313; i<(56313+24); i++)
			System.out.print(data.get(i) + ",");
		System.out.println("Values starting at index = " + 56313);
		*/
		
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
			//ptrnHandler.storePattern(config1.getCluster(), pguid, patternList);
		Pattern patternObj = new Pattern(config2.getCluster(), pguid, patternList);
		RESTFactory.getPatternProxy(addr).storePatternJSON(patternObj);
		
		//List<String> fetchedPatternList = ptrnHandler.fetchPattern(config1.getCluster(), pguid);
		Pattern fetchedPatternObj = RESTFactory.getPatternProxy(addr).fetchStoredPatternJSON(config2.getCluster(), pguid);
		
		Double[] fetchedPattern = new Double[pattern.length];
		for (int i=0; i< fetchedPatternObj.getVals().size(); i++)
			fetchedPattern[i] = Double.valueOf(fetchedPatternObj.getVals().get(i));
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
		//ptrnHandler.processMatches(request);
		//Matches result = request;
		Matches result = RESTFactory.getPatternProxy(addr).fetchMatchesJSON(request);
		
		for (double dist : result.getMatchResults().keySet()) {
			for (Match m : result.getMatchResults().get(dist)) {
				System.out.println("Found match at offset = " + m.getValues().firstKey() + " at distance = " + m.getDistance() 
					+ " from cluster = " + m.getCluster() + " and ln = " + m.getLn() + " for id = " + m.getId());
			}
		}
		//System.in.read();
	}

	@org.junit.Test
	public void verifyOperatorParsing() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//HandlerFactory factory = new HandlerFactory();
		//IOperatorHandler opHandler = factory.getNewOperatorHandler();
		List<Integer> data = getSampleZipfData();
		long startts = config2.getStartDate().getMillis();
		long endts = config2.getEndDate().getMillis();
		String x = "'" + config2.getCluster() + "#" + config2.getLogicalNode() + "#" + config2.getGuid() + "'";
		List<String> functions = new ArrayList<String>();
		functions.add("x - 3 * sin ( x ) "); 
		functions.add(" x * x ");
		Map<String,String> vars = new HashMap<String,String>();
		vars.put(x, "x");
		Operators request = new Operators();
		request.setVars(vars);
		request.setFuncs(functions);
		request.setStart(startts);
		request.setEnd(endts);
		//List<SortedMap<Long,Object>> results = opHandler.fetchComputedTimeSeries(request.getVars(), request.getFuncs(), request.getStart(), 
		//		request.getStartHfOffset(), request.getEnd(), request.getEndHfOffset());
		
		request = RESTFactory.getOperatorProxy(addr).fetchComputedTimeSeries(request);
		List<SortedMap<Long,Object>> results = request.getComputedResults();
		List<SortedMap<Long,Object>> computedResults = new ArrayList<SortedMap<Long,Object>>();
		for (int i=0; i< functions.size(); i++)
			computedResults.add(new TreeMap<Long,Object>());
		assertEquals(results.size(), functions.size());
		assertEquals(results.get(0).size(), data.size());
		
		
		for (int i = 0; i < data.size(); i++) {
			computedResults.get(0).put( new Long(i), (data.get(i) - 3 * Math.sin(data.get(i) * 1.0)) );
			computedResults.get(1).put( new Long(i), (data.get(i) * data.get(i) * 1.0) );
		}
		
		for (int i=0; i<functions.size(); i++)
			assertEquals(computedResults.get(i), results.get(i));
	}
}