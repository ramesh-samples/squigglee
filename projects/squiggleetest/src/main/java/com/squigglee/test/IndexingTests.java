// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

//import static org.junit.Assert.*;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.interfaces.ISchemaHandler;
import com.squigglee.core.sketch.LocalitySensitiveHasher;
import com.squigglee.core.synthetic.Zipf;
import com.squigglee.core.utility.IndexIO;
import com.squigglee.core.utility.IndexIORedo;
import com.squigglee.core.utility.MappedDataHandler;
import com.squigglee.storage.mbb.ISchemaHandlerImpl;
 
public class IndexingTests {
	protected static DistanceMeasure measure = new EuclideanDistance();
	protected static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	protected static SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
	
	@org.junit.Test
	public void IndexingDistribution() throws Exception {
		//Bijection bijection = new Bijection();
		System.out.println(String.format("%.0f",Math.pow(20.0, 12.0)));
		System.out.println(Long.parseLong(String.format("%.0f",Math.pow(20.0, 12.0)*100)));
		System.out.println();
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//IndexIO indexIO = new IndexIO();
		int size = 24;
		String index = "ptrn_24_1_10_8_1";
		LocalitySensitiveHasher lsh = new LocalitySensitiveHasher(1,0);
		lsh.setValuesFromTableName(0, index);
		Zipf generator = new Zipf(1000,1.0);
		DateTime start = DateTime.now();
		int sampleSize = 10000;
		int[] values = generator.sample(sampleSize);
		DateTime end = DateTime.now();
		System.out.println("Time to generate " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
		
		start = DateTime.now();
		for (int i = 0; i < values.length-size; i++) {
			double[] slice = new double[size];
			for (int j = 0; j < size; j++)
				slice[j] = values[i + j];
			lsh.index(i, slice);
		}
		end = DateTime.now();
		System.out.println("Time to index " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
		
		int totalCount = 0;
		Map<Integer,Integer> counts = new HashMap<Integer, Integer>();
		for (Integer hash : lsh.getLookupMap().keySet()) {
			//String[] tokens = hashes.split("#");
			//for (int i = 0; i < tokens.length - 1; i++) {
				totalCount++;
				//int val = Integer.parseInt(tokens[i]);
				if (!counts.containsKey(hash))
					counts.put(hash, 1);
				else
					counts.put(hash, counts.get(hash) + 1);
			//}				
		}
		
		System.out.println(counts);
		int rangeCount20 = 0;
		int rangeCount10 = 0;
		int rangeCount5 = 0;
		int rangeCount3 = 0;
		int rangeCount2 = 0;
		System.out.println("Total counts of hash indexes computed = " + totalCount);
		for (int val : counts.keySet()) {
			totalCount++;
			if (val >= -20 && val <= 20)
				rangeCount20 += counts.get(val);
			if (val >= -10 && val <= 10)
				rangeCount10 += counts.get(val);
			if (val >= -5 && val <= 5)
				rangeCount5 += counts.get(val);
			if (val >= -3 && val <= 3)
				rangeCount3 += counts.get(val);
			if (val >= -2 && val <= 2)
				rangeCount2 += counts.get(val);
		}
		System.out.println("Range Count 20 = " + rangeCount20 + "; Range Count 10 = " + rangeCount10 
				+ "; Range Count 5 = " + rangeCount5 + "; Range Count 2 = " + rangeCount2);
		System.out.println("Range Count 20 % = " + rangeCount20*100.0/totalCount + "; Range Count 10 % = " 
				+ rangeCount10*100.0/totalCount + "; Range Count 5 % = " + rangeCount5*100.0/totalCount 
				+ "; Range Count 3 % = " + rangeCount3*100.0/totalCount + "; Range Count 2 % = " + rangeCount2*100.0/totalCount);
	}
	
	@org.junit.Test
	public void IndexingPreAllocateMultiStorage() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		String storagePath = LocalNodeProperties.getStoragePath();
		
		String indexName = "ptrn_24_1_100_8_1";
		long id = 12;
		String path = storagePath + "/indexes" + "/" + indexName + "_" + id + "_multi" ;
		IndexIORedo indexIORedo = new IndexIORedo(100, LocalNodeProperties.getStoragePath());
		DateTime start = DateTime.now();
		File f = new File(path);
		indexIORedo.preAllocateMultiFile(f);
		DateTime end = DateTime.now();
		System.out.println("Time to pre-allocate multi file = " + f.getAbsolutePath() + " = "
				+ (new Interval(start, end)).toDurationMillis() + " millis");
		System.in.read();
	}
	
	@org.junit.Test
	public void IndexingPreAllocateDataFile() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		String storagePath = LocalNodeProperties.getStoragePath();
		
		String dataType = "int";
		long id = 12;
		String path = storagePath + "/data" + "_" + id ;
		IndexIORedo indexIORedo = new IndexIORedo(100, LocalNodeProperties.getStoragePath());
		DateTime start = DateTime.now();
		File f = new File(path);
		indexIORedo.preAllocateDataFile(f, dataType);
		DateTime end = DateTime.now();
		System.out.println("Time to pre-allocate multi file = " + f.getAbsolutePath() + " = "
				+ (new Interval(start, end)).toDurationMillis() + " millis");
		System.in.read();
	}
	
	@org.junit.Test
	public void IndexingTest2() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//IIndexService indexService = ServiceFactory.getIndexService();
		int size = 24;
		String index = "ptrn_24_1_10_4_1_111";
		LocalitySensitiveHasher lsh = new LocalitySensitiveHasher(1,0);
		lsh.setValuesFromTableName(0, index);
		Zipf generator = new Zipf(1000,1.0);
		for ( int iter = 0; iter < 10; iter++) {
			DateTime start = DateTime.now();
			int sampleSize = 1000;
			int[] values = generator.sample(sampleSize);
			DateTime end = DateTime.now();
			System.out.println("Time to generate " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			
			start = DateTime.now();
			for (int i = 0; i < values.length-size; i++) {
				double[] slice = new double[size];
				for (int j = 0; j < size; j++)
					slice[j] = values[i + j];
				lsh.index(i, slice);
			}
			end = DateTime.now();
			System.out.println("Time to index " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			
			//Map<Integer,List<Long>> indexedValues = lsh.getLookupMap();
			//start = DateTime.now();
			//indexService.writeIndex("TestCluster", 0, index, 0, indexedValues);
			//end = DateTime.now();
			//System.out.println("Time to store " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			//System.in.read();
			//Thread.sleep(2000);
			//start = DateTime.now();
			//for (int hashes : indexedValues.keySet()) {
			//	List<Long> vals = indexService.getVals("TestCluster", 0, index, 0, hashes);
			//	for (long val : vals)
			//		assert  indexedValues.get(hashes).contains(val);
			//}
			//end = DateTime.now();
			//System.out.println("Time to retrieve and verify " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
		}
	}

	@org.junit.Test
	public void IndexingTest3() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//IndexIORedo indexIORedo = new IndexIORedo(16, LocalNodeProperties.getStoragePath());
		int size = 24;
		String index = "ptrn_24_1_100_8_1";
		//long id = 12;
		LocalitySensitiveHasher lsh = new LocalitySensitiveHasher(1,0);
		lsh.setValuesFromTableName(0, index);
		Zipf generator = new Zipf(1000,1.0);
		for ( int iter = 0; iter < 1; iter++) {
			DateTime start = DateTime.now();
			int sampleSize = 1000;
			int[] values = generator.sample(sampleSize);
			DateTime end = DateTime.now();
			System.out.println("Time to generate " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			
			start = DateTime.now();
			for (int i = 0; i < values.length - size + 1; i++) {
				double[] slice = new double[size];
				for (int j = 0; j < size; j++)
					slice[j] = values[i + j];
				lsh.index(i, slice);
			}
			end = DateTime.now();
			System.out.println("Time to index " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			
			//Map<Integer,List<Long>> indexedValues = lsh.getLookupMap();
			//start = DateTime.now();
			//indexIORedo.writeIndex(index, id, indexedValues);
			
			//end = DateTime.now();
			//System.out.println("Time to store " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			
			System.in.read();
			
			//start = DateTime.now();
			//for (String hashes : indexedValues.keySet()) {
			//	List<Long> vals = indexIORedo.getVals(index, 12, hashes);
			//	for (Long v : indexedValues.get(hashes))
			//		assertTrue(vals.contains(v));
			//}
			//end = DateTime.now();
			//System.out.println("Time to retrieve and verify " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			
		}
	}

	@org.junit.Test
	public void DeleteIndexTest() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//IIndexService indexService = ServiceFactory.getIndexService();
		String index = "ptrn_24_1_10_8_1_111";
		DateTime start = DateTime.now();
		//int ln = 0;
		long id = 0;
		//indexService.deleteIndex("TestCluster", ln, index, id);
		DateTime end = DateTime.now();
		System.out.println("Time to delete index " + index + " and id = " + id + " = " + (new Interval(start, end)).toDurationMillis() + " millis");	
	}

	@org.junit.Test
	public void IndexingInitializeStorage() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		IndexIO indexIO = new IndexIO(100, LocalNodeProperties.getStoragePath());
		int depth = 6;
		int startIndex = -20;
		int endIndex = 20;
		DateTime start = DateTime.now();
		indexIO.setupStorage(startIndex, endIndex, depth);
		System.in.read();
		DateTime end = DateTime.now();
		System.out.println("Time to generate setup storage of depth " + depth + " and range [" + startIndex + "," + endIndex + "] = " 
				+ (new Interval(start, end)).toDurationMillis() + " millis");
	}

	@org.junit.Test
	public void IndexingTest1() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//IndexIO indexIO = new IndexIO(100, LocalNodeProperties.getStoragePath());
		int size = 24;
		String index = "ptrn_24_1_20_4_1";
		LocalitySensitiveHasher lsh = new LocalitySensitiveHasher(1,0);
		lsh.setValuesFromTableName(0, index);
		Zipf generator = new Zipf(1000,1.0);
		for ( int iter = 0; iter < 10; iter++) {
			DateTime start = DateTime.now();
			int sampleSize = 10000;
			int[] values = generator.sample(sampleSize);
			DateTime end = DateTime.now();
			System.out.println("Time to generate " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			
			start = DateTime.now();
			for (int i = 0; i < values.length-size; i++) {
				double[] slice = new double[size];
				for (int j = 0; j < size; j++)
					slice[j] = values[i + j];
				lsh.index(i, slice);
			}
			end = DateTime.now();
			System.out.println("Time to index " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			
			//Map<Integer,List<Long>> indexedValues = lsh.getLookupMap();
			//start = DateTime.now();
			//indexIO.writeIndex(index, 0, indexedValues);
			
			//end = DateTime.now();
			//System.out.println("Time to store " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			
			System.in.read();
			
			//start = DateTime.now();
			//for (String hashes : indexedValues.keySet()) {
			//	List<Long> vals = indexIO.getVals(index, 0, hashes);
			//	assertEquals(vals, indexedValues.get(hashes));
			//}
			//end = DateTime.now();
			//System.out.println("Time to retrieve and verify " + sampleSize + " samples = " + (new Interval(start, end)).toDurationMillis() + " millis");
			
		}
	}

	@org.junit.Test
	public void IndexingPreAllocateStorage() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		String storagePath = LocalNodeProperties.getStoragePath();
		
		String indexName = "ptrn_24_1_10_8_1";
		long id = 12;
		String path = storagePath + "/indexes" + "/" + indexName + "_" + id;
		IndexIORedo indexIORedo = new IndexIORedo(100, LocalNodeProperties.getStoragePath());
		DateTime start = DateTime.now();
		File f = new File(path);
		indexIORedo.preAllocateFile(f);
		DateTime end = DateTime.now();
		System.out.println("Time to pre-allocate file = " + f.getAbsolutePath() + " = "
				+ (new Interval(start, end)).toDurationMillis() + " millis");
		System.in.read();
	}

	@org.junit.Test
	public void TestBuffers() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		int size = 24;
		//String index = "ptrn_24_1_100_8_1";
		String index = "ptrn_24_1_25_8_1";
		long id = 12;
		LocalitySensitiveHasher lsh = new LocalitySensitiveHasher(1,0);
		lsh.setValuesFromTableName(0, index);
		Zipf generator = new Zipf(1000,1.0);
		
		for (int iter = 0; iter < 360; iter++) {
			int sampleSize = 10000;
			int[] values = generator.sample(sampleSize);
			MasterData md = new MasterData("TestCluster", id, 0, "TestParameter", DateTimeHelper.getNowStartts(), Frequency.MILLIS, "int", index);
			ISchemaHandler schemaHandler = new ISchemaHandlerImpl();
			
			boolean created = schemaHandler.createSchema(md);
			//if (created)
			//	Thread.sleep(10000);
			created = schemaHandler.createPatternIndexTables(md.getCluster(), md.getId(), index, null);
			if (created)
				Thread.sleep(30000);
			
			SortedMap<Long,Object> data = new TreeMap<Long, Object>();
			for (int i = iter*sampleSize; i< (iter*sampleSize + values.length); i++)
				data.put(new Long(i), values[i % sampleSize]);
			MappedDataHandler.writeData(md, data);
			
			SortedMap<Long,Object> storedData = MappedDataHandler.readData(md, iter*sampleSize, (iter*sampleSize + values.length) - 1);
			for (int i=iter*sampleSize; i<(iter*sampleSize + values.length); i++)
				assertEquals((Integer) values[i % sampleSize], (Integer) storedData.get((long) i));
			
			for (int i = iter*sampleSize; i < ((iter*sampleSize + values.length) - size + 1); i++) {
				double[] slice = new double[size];
				for (int j = 0; j < size; j++)
					slice[j] = values[i % sampleSize + j];
				lsh.index(i, slice);
			}
			
			//Map<Integer,List<Long>> indexedValues = lsh.getLookupMap();
			//MappedIndexHandler.writeIndex(md, index, indexedValues);
			
			System.in.read();

			//int counter = 0;
			//int failedCounter = 0;
			//for (String hashes : indexedValues.keySet()) {
				//counter++;
			//	List<Long> vals = MappedIndexHandler.readIndex(md, index, hashes);
			//	boolean failed = false;
			//	for (Long v : indexedValues.get(hashes)) {
			//		if (!vals.contains(v)) {
			//			failed = true;
			//		}
					//assertTrue(vals.contains(v));
			//	}
			//	if (failed) {
			//		failedCounter++;
					//System.out.println("Counter:" + counter + " -- Failed to match index for hash = " + hashes + " with hashIndex = " + MurmurHash.hash32(hashes)
					//	+ ", actual indexed values = " 
					//	+ indexedValues.get(hashes) + ", retrieved values = " + vals);
			//	}
			//}
			//System.out.println("Failed counter = " + failedCounter + " of total count = " + indexedValues.keySet().size() 
			//		+ " (" + failedCounter*100.0/indexedValues.keySet().size() + " %)");
			//System.out.println("**************************************Done with iteration " + iter);
			//lsh.getLookupMap().clear();
		}
	}
}