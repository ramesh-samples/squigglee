// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

//import static org.junit.Assert.*;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.stat.StatUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.squigglee.client.perf.RandomReadTest;
import com.squigglee.coord.interfaces.IConfigService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.Match;
import com.squigglee.core.entity.Matches;
import com.squigglee.core.entity.Pattern;
import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.interfaces.IIndexHandler;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.interfaces.IPatternHandler;
import com.squigglee.core.interfaces.ISampledDataHandler;
import com.squigglee.core.interfaces.ISketchHandler;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.sketch.EmpiricalPatternAnalyzer;
import com.squigglee.core.sketch.LocalitySensitiveHasher;
import com.squigglee.core.interfaces.HandlerFactory;
 
public class AdHocTests {
	protected static DistanceMeasure measure = new EuclideanDistance();
	protected static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	protected static SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
	
	@org.junit.Test
	public void verifyRandomReads() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		RandomReadTest readTest = new RandomReadTest(2000, 10000, false);
		readTest.runRandomReads("127.0.0.1");
		//System.in.read();
	}
	
	//@org.junit.Test
	public void testDateTimeParsing() throws Exception {
		System.out.println(DateTimeHelper.getSampleStartOfToday());
		System.out.println(DateTimeHelper.getSampleEndOfToday());
		
		DateTime val1 = DateTimeHelper.parseDateString("2015-02-11T00:00:00.000Z");
		DateTime val2 = DateTimeHelper.parseDateString("2015-02-11T23:59:59.999Z");
		
		System.out.println(val1);
		System.out.println(val2);
		
	}
	
	//@org.junit.Test
	public void testLogicalNode() throws Exception {
		System.out.println("Logical node for id = " + 0 + " = " + TimeSeriesConfig.getLogicalNode(0L));
		System.out.println("Logical node for id = " + 1 + " = " + TimeSeriesConfig.getLogicalNode(1L));
		System.out.println("Logical node for id = " + 1000000000L + " = " + TimeSeriesConfig.getLogicalNode(1000000000L));
		System.out.println("Logical node for id = " + 1000000001L + " = " + TimeSeriesConfig.getLogicalNode(1000000001L));
	}
	
	//@org.junit.Test
	public void verifySketch() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		IDataHandler dataHandler = HandlerFactory.getDataHandler();
		ISketchHandler sketchHandler = HandlerFactory.getSketchHandler();
		ISampledDataHandler sdHandler = HandlerFactory.getSampledDataHandler();
		
		Stats stats = sketchHandler.statistics("TestCluster", 0, "Parameter1");
		
		List<String> heavyHitters = new ArrayList<String>();
		Map<Long, Object> hh = stats.getHeavyHitters();
		if (hh != null) {
			for (Long o : hh.keySet())
				heavyHitters.add("" + DynamicTypeTranslator.convertDoubleToNumeric( (Double) hh.get(o), "int"));
		}
		
		long pointQueryResult = sketchHandler.pointQuery("TestCluster", 0, "Parameter1", 1);
		System.out.println("Point query result = " + pointQueryResult);
		
		long rangeQueryResult = sketchHandler.rangeQuery("TestCluster", 0, "Parameter1", 1, 1000);
		System.out.println("Range query result = " + rangeQueryResult);
		
		dataHandler.reset("int");
		SortedMap<Integer,Long> sketchHistogram = sketchHandler.getSketchHistogram("TestCluster", 0,"Parameter1",10);
		System.out.println("Sketch Histogram");
		System.out.println(sketchHistogram);
		sdHandler.reset("int");
		DateTime start = new DateTime(sdf.parse("2015-02-08T00:00:00.000-0000").getTime(),DateTimeZone.UTC);
		DateTime end = new DateTime(sdf.parse("2015-02-08T00:00:00.999-0000").getTime(),DateTimeZone.UTC);
		SortedMap<Integer,Long>  sampledDataHistogram = sdHandler.getSampledDataHistogram("TestCluster", 0, "Parameter1", 10, 
				start.getMillis(), 0, end.getMillis() + 999, 0, 1000, sketchHandler);
		System.out.println("Sampled Data Histogram subsample size 1000");
		System.out.println(sampledDataHistogram);
		dataHandler.shutdown();
		sketchHandler.shutdown();
		sdHandler.shutdown();
	}
	
	@org.junit.Test
	public void verifyIndexDelete() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		IIndexHandler idxHandler = HandlerFactory.getIndexHandler();
		IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
		
		idxHandler.updateIndex("TestCluster", 0, "aud_cad_ask", "ptrn_16_10000_100_8_10000", true);
		List<TimeSeriesConfig> list = mdHandler.getMasterData("TestCluster", 0, "aud_cad_ask");
		String oldIndex = list.get(0).getIndexes();
		assertEquals(oldIndex, "");
		idxHandler.shutdown();
		mdHandler.shutdown();
	}
	
	@org.junit.Test
	public void verifyPatternIndexing() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		IDataHandler dataHandler = HandlerFactory.getDataHandler();
		IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
		IPatternHandler ptrnHandler = HandlerFactory.getPatternHandler();
		DateTime start = DateTimeHelper.parseDateString("2014-01-05T00:00:00.000Z");
		DateTime end = DateTimeHelper.parseDateString("2014-01-05T23:59:59.999Z"); 

		dataHandler.reset("double");
		//Map<Integer,Object> data = dataHandler.fetchTimeSeries(46, "ks_data_0_int", 0, 999);
		MasterData md = mdHandler.getMasterData("TestCluster", 0);
		SortedMap<Long,Object> data = dataHandler.fetchTimeSeriesLimit(md, 0, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, 1000, false); 

		//List<Double> data = getSampleEKGData(); // already stored 
		//String pguid = java.util.UUID.randomUUID().toString();
		int startIndex = 82;
		//the 11 through 26 values in the test data set 
		Double[] pattern = new Double[24];
		double[] ptrn = new double[24];
		Object[] keyArray = data.keySet().toArray();
		List<String> stringPattern = new ArrayList<String>();
		for (int i=0; i<pattern.length; i++) {
			Integer key = (Integer) keyArray[startIndex + i];
			ptrn[i] = Double.parseDouble(data.get(key).toString());
			stringPattern.add("" + data.get(key).toString());
			pattern[i] = ptrn[i];
		}
		List<TimeSeriesConfig> idList = new ArrayList<TimeSeriesConfig>();
		TimeSeriesConfig cfg1 = new TimeSeriesConfig("TestCluster", "aud_cad_ask",0,Frequency.MILLIS,"double","", start, end);
		idList.add(cfg1);
		ptrnHandler.reset("double");
		double radius = 2.0;
		int topk = 6;
		
		Matches request = new Matches();
		request.setPattern(new Pattern(stringPattern));
		request.setRadius(radius);
		request.setTopk(topk);
		request.setRequestDomain(idList);
		ptrnHandler.processMatches(request);
		for (double dist : request.getMatchResults().keySet()) {
			for (Match m : request.getMatchResults().get(dist)) {
				System.out.println("Found match at offset = " + m.getValues().firstKey() + " at distance = " + m.getDistance() 
					+ " from cluster = " + m.getCluster() + " and ln = " + m.getLn() + " for id = " + m.getId());
			}
		}
		
		dataHandler.shutdown();
		ptrnHandler.shutdown();
	}

	@org.junit.Test
	public void verifyMatchQuery() throws Exception {
		
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		IDataHandler dataHandler = HandlerFactory.getDataHandler();
		IIndexHandler idxHandler = HandlerFactory.getIndexHandler();
		IPatternHandler ptrnHandler = HandlerFactory.getPatternHandler();
		IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
	
		List<TimeSeriesConfig> cfgList = mdHandler.getMasterData("TestCluster", 0, "ZIPF_Sample1");
		dataHandler.reset("int");
		MasterData md = mdHandler.getMasterData("TestCluster", 2);
		SortedMap<Long,Object> tsData = dataHandler.fetchTimeSeriesLimit(md, 0, 60000, 1000, false);
		int startIndex = 103;
		//the 11 through 26 values in the test data set 
		Double[] pattern = new Double[24];
		double[] ptrn = new double[24];
		List<String> stringPattern = new ArrayList<String>();
		for (int i=0; i<pattern.length; i++) {
			pattern[i] = Double.parseDouble(tsData.get(startIndex + i).toString());
			ptrn[i] = pattern[i];
			stringPattern.add(tsData.get(startIndex + i).toString());
		}
		double radius = 2.0;
		int topk = 3;
		
		Matches request = new Matches();
		request.setPattern(new Pattern(stringPattern));
		request.setRadius(radius);
		request.setTopk(topk);
		request.setRequestDomain(cfgList);
		ptrnHandler.processMatches(request);
		for (double dist : request.getMatchResults().keySet()) {
			for (Match m : request.getMatchResults().get(dist)) {
				System.out.println("Found match at offset = " + m.getValues().firstKey() + " at distance = " + m.getDistance() 
					+ " from cluster = " + m.getCluster() + " and ln = " + m.getLn() + " for id = " + m.getId());
			}
		}
		
		dataHandler.shutdown();idxHandler.shutdown();ptrnHandler.shutdown();mdHandler.shutdown();
	}

	@org.junit.Test
	public void verifyIndexingJob() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		IConfigService configService = ServiceFactory.getConfigurationService();
		Map<String, List<TimeSeriesConfig>> map = configService.getConfig("TestCluster", 0);
		System.out.println(map);
		for (String guid : map.keySet()) {
			if (guid.equalsIgnoreCase("Parameter1")) {
				IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
				mdHandler.deleteMasterData(map.get(guid).get(0));
			}
		}
		//
		//IStatusService statusService = new IStatusServiceImpl();
		//statusService.initialize();
		// update the method call
		//PatternIndexingTask task = new PatternIndexingTask(2,"ks_data_0_int","int", "ptrn_24_1_100_8_1", factory, statusService, LocalNodeProperties.getIndexChunkSize(),
		//		LocalNodeProperties.getIndexNumChunks(), 0, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, null, LocalNodeProperties.getIndexTimerInterval(), true, 1, 0);
		
		//task.run();
	}
	
	@org.junit.Test
	public void verifyPatternIndexingAlgorithm2() throws Exception {
		long id = 0;
		int dimension = 24;
		List<Integer> data = TestBase.getSampleZipfData(); // already stored 
		double radius = 2.0;
		int topk = 50;
		int size = 10;
		int projections = 100;
		int b = 1;
		//List<String> indexes = new ArrayList<String>();
		//indexes.add("ptrn_24_1_100_10_1");
		//indexes.add("ptrn_16_1_100_10_1");
		//indexes.add("ptrn_8_1_100_10_1");
		//indexes.add("ptrn_4_1_100_10_1");
		//indexes.add("ptrn_24_1_1_10_1");
		//indexes.add("ptrn_24_1_100_4_1");
		//indexes.add("ptrn_24_100_100_10_1");
		//indexes.add("ptrn_24_1_100_10_100");
		//for (int dim : new int[]{24,16,8}) {
		for (int scalar : new int[]{10,100,1000,10000}) {
			EmpiricalPatternAnalyzer epa = new EmpiricalPatternAnalyzer(id, dimension, radius, topk, data);
			epa.initialize();
			epa.reset();
			epa.addParameter("ptrn_" + dimension + "_" + b + "_" + projections + "_" + size + "_" + scalar);
			Map<String, EmpiricalPatternAnalyzer.Stats> results = epa.getResults();
			for (String index : results.keySet()) {
				System.out.println("Results for index = " + index);
				System.out.println("Precision = " + results.get(index).getPrecision());
				System.out.println("Recall = " + results.get(index).getRecall());
				System.out.println("Accuracy = " + results.get(index).getAccuracy());
			}
		}
	}

	@org.junit.Test
	public void verifyPatternIndexingAlgorithm() throws Exception {
		List<Integer> data = TestBase.getSampleZipfData(); // already stored 
		
		
		SortedMap<Double,String> algorithmAccuracy = new TreeMap<Double,String>(Collections.reverseOrder());
		
		DistanceMeasure measure = new EuclideanDistance(); 
		double radius = 20.0;
		int topk = 10;
		
		int dimension = 24;
		int bucketWidth = 1;
		int projections = 100;
		int size = 10;
		int scalar = 1;
		int id = 0;
		//String index = "ptrn_24_1_100_10_1"
		String index = "ptrn_" + dimension + "_" + bucketWidth + "_" + projections + "_" + size + "_" + scalar;
		
		//int[] testPatterns = new int[data.size() - dimension + 1];
		//testPatterns = new int[]{0,2,59950};
		double[][] vals = new double[data.size() - dimension + 1][];
		double[][] normvals = new double[data.size() - dimension + 1][];
		for (int i = 0; i < vals.length; i++) {
			vals[i] = new double[dimension];
			for (int j=0; j< vals[i].length; j++) {
				vals[i][j] = data.get(i + j);
			}
			normvals[i] = StatUtils.normalize(vals[i]);
		}
		
		LocalitySensitiveHasher lsh = new LocalitySensitiveHasher(1,0);
		lsh.setValuesFromTableName(id, index);
		for (int i = 0; i< vals.length; i++)
				lsh.index(i, vals[i]);
	
		
		double globalAccuracy = 0.0;
		for (int i = 0; i < vals.length; i++) {
			List<Long> candidates = lsh.getIndexedNeighbors(normvals[i]);
			SortedMap<Double,List<Match>> approxMatches = new TreeMap<Double, List<Match>>();
			SortedMap<Double,List<Match>> trueMatches = new TreeMap<Double, List<Match>>();
			for (long c : candidates) {
				double[] slice = TestBase.getSlice((int) c, data, dimension);
				double dist = measure.compute(normvals[i], StatUtils.normalize(slice));
				if (dist > radius)
					continue;
				SortedMap<Long,Object> matchValues = new TreeMap<Long,Object>();
				for (int k=0; k < slice.length; k++)
					matchValues.put(c + k, slice[k]);
				Match m = new Match("TestCluster", 0, "ZIPF_Sample1", "int", "millis", 
						DateTime.now().withTimeAtStartOfDay().getMillis(),	matchValues, dist);
				//Match m = new Match(id, matchValues, dist);
				if (!approxMatches.containsKey(dist))
					approxMatches.put(dist, new ArrayList<Match>());
				approxMatches.get(dist).add(m);
			}
	
			for (long c = 0; c< data.size() - dimension + 1; c++) {
				double[] slice = TestBase.getSlice((int) c, data, dimension);
				double dist = measure.compute(normvals[i], StatUtils.normalize(slice));
				if (dist > radius)
					continue;
				SortedMap<Long,Object> matchValues = new TreeMap<Long,Object>();
				for (int k=0; k < slice.length; k++)
					matchValues.put(c + k, slice[k]);
				Match m = new Match("TestCluster", 0, "ZIPF_Sample1", "int", "millis", 
						DateTime.now().withTimeAtStartOfDay().getMillis(),	matchValues, dist);
				//Match m = new Match(id, matchValues, dist);
				if (!trueMatches.containsKey(dist))
					trueMatches.put(dist, new ArrayList<Match>());
				trueMatches.get(dist).add(m);
			}
			
			double accuracy = 0.0;	// weighted empirical accuracy measure --> by number of matches retrieved 
			for (int k = 1; k <= topk; k++) {
				List<Match> indexedMatches = TestBase.getTopkMatches(approxMatches, k);
				List<Match> exactMatches = TestBase.getTopkMatches(trueMatches, k);
				int accurateCount = 0;
				for (Match m : indexedMatches) {
					for (Match em : exactMatches) {
						if (em.getValues().firstKey().equals(m.getValues().firstKey())) {
							accurateCount++;
							break;
						}
					}
				}
				double proportionCorrect = accurateCount * 1.0 / exactMatches.size();
				//System.out.println("Proportion correct for k = " + k + " = " + proportionCorrect + " for accurateCount = " + accurateCount + " with exactCount = " + exactMatches.size());
				accuracy += proportionCorrect * (topk - k + 1);	
				
			}
			accuracy = accuracy * 200.0 / (topk*(topk + 1));		// weighting total = topk*(topk+1)/2
			//System.out.println("Accuracy for pattern starting at index " + i + " for top " + topk + " closest matches = " + String.format("%.2f",accuracy) + "%");
			globalAccuracy += accuracy;
			if (i % 100 == 0)
				System.out.println("Completed index = " + i);
		}
		algorithmAccuracy.put(globalAccuracy / vals.length, index);
		System.out.println(algorithmAccuracy);
		//System.in.read();
	}

	@org.junit.Test
	public void verifyCEP() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//(new squiggleecep.TestCEP()).test1();
	}
	

}