// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

//import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.api.restproxy.ConfigurationProxy;
import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.utility.ZooKeeperFactory;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;
import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.Match;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.jobs.indexing.IndexingMaster;
import com.squigglee.jobs.indexing.IndexingWorker;
import com.squigglee.jobs.sync.SyncWorker;

/**
 * Remember to bootstrap master data i.e. create "ks_md" keyspace & create the "md" table in that keyspace
 * @author AgnitioWorks
 *
 */
public class TestBaseRest {
	
	protected static TestUtility utility = null;
	protected static String vdbGlobalName = "TIMESERIESGLOBAL";
	protected static String vdbLocalName = "TIMESERIES";
	protected static String localBulkInsertStatement = "INSERT INTO TimeSeriesViewModel.BULKDATA" + 
							" (ln,id,datatype,bdata) VALUES (?,?,?,?)";
	protected static TimeSeriesConfig config1 = null;
	protected static TimeSeriesConfig config2 = null;
	protected static Map<Frequency,TimeSeriesConfig> freqConfigs = new HashMap<Frequency,TimeSeriesConfig>();
	protected static DistanceMeasure measure = new EuclideanDistance();
	
	protected static String cluster = null;
	protected static int ln = 0;
	protected static String addr = null;
	
	public static void setUpEnvironment() throws TimeSeriesException {
		utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		HandlerFactory.initialize("com.squigglee.storage.mbb");
		RESTFactory.initialize("/squiggleerestui", 8080, "http", 20000, 20000);
		cluster = LocalNodeProperties.getClusterName();
		ln = LocalNodeProperties.getNodeLogicalNumber();
		addr = "127.0.0.1";
	}
	
	public static void setUpHandlers() throws TimeSeriesException {
	}
	
	public static void setUpStorage() throws Exception {
		IDataService dataService = com.squigglee.coord.utility.ServiceFactory.getDataService();
		//dataService.deleteNode(cluster, ln);
		//statusHandler.deleteNode(ln);
		//dataService.deleteCluster(cluster);
		cleanData(dataService);
		//dataService.setupCluster(cluster);
		RESTFactory.getConfigurationProxy(addr).setupCluster(cluster);
		//configRestClient.setupCluster(cluster);
		//dataService.setNode(cluster, ln, "127.0.0.1", "local", "local",	true, true, "TimeSeriesNode_0", ln, 100, "Medium");
		RESTFactory.getConfigurationProxy(addr).updateNode(cluster, ln, "127.0.0.1", "local", "local", "TimeSeriesNode_0", true, true, ln, 100, "Medium");
		//statusHandler.updateNode(ln, "127.0.0.1", "local", "local", "TimeSeriesNode_0", true, true, ln, 100, "Medium");
		
		configureSampleData();
		createMasterData();
		System.in.read();
		//Thread.sleep(2000);
		indexSampleData();
		Thread.sleep(2000);
		//addSampleEKGData();
		//Thread.sleep(20000);
		addSampleZIPFData();
		Thread.sleep(20000);
		System.in.read();
	}
	
	public static void cleanUpStorage() throws TimeSeriesException {

		for (TimeSeriesConfig cfg : new TimeSeriesConfig[]{config1, config2}) {
			RESTFactory.getConfigurationProxy(addr).dropIndexJSON(cfg);
			//idxHandler.updateIndex(cfg.getCluster(), cfg.getLogicalNode(), cfg.getGuid(), cfg.getIndexes(), true);
		}
		
		for (TimeSeriesConfig cfg : freqConfigs.values()) {
			RESTFactory.getConfigurationProxy(addr).dropIndexJSON(cfg);
			//idxHandler.updateIndex(cfg.getCluster(), cfg.getLogicalNode(), cfg.getGuid(), cfg.getIndexes(), true);
		}
		
		//dataService.deleteNode(cluster, ln);
		//dataService.deleteCluster(cluster);
	}
	
	@BeforeClass
	public static void setUpClass() throws Exception {
		setUpEnvironment();
		setUpHandlers();
		setUpStorage();
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
		
	}

	@Before
	public void setUp() throws Exception {
	}
	
	@After
	public void tearDown() throws Exception {
		
	}
	
	protected static void cleanData(IDataService dataService) throws Exception {
		
		/*
		File file = new File(LocalNodeProperties.getStoragePath()); 
		if (file.isDirectory()) {
			String[] list = file.list();
			for (String nm : list)
				(new File(LocalNodeProperties.getStoragePath() + "/" + nm)).delete();
		}
		*/
		
		ZooKeeper zk = ZooKeeperFactory.getLocalZooKeeper();
		if (zk.exists(TsrConstants.ROOT_PATH, false) == null)
			return;
		
		for (String entry : new String[]{"indexes","syncqueue","syncs","tasks","taskqueue"}) {
			if (zk.exists(TsrConstants.ROOT_PATH + "/" + cluster + "/" + entry, false) != null)
				for (String subPath : zk.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/" + entry, false))
					dataService.deleteRecursive(TsrConstants.ROOT_PATH + "/" + cluster + "/" + entry + "/" + subPath);
		}
		for (String entry : new String[]{"assigned","assignedqueue"}) {
			if (zk.exists(TsrConstants.ROOT_PATH + "/" + cluster + "/" + entry, false) != null)
				for (String worker : zk.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/" + entry, false)) {
					for (String assignedTask : zk.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/" + entry + "/" + worker, false))
						dataService.deleteRecursive(TsrConstants.ROOT_PATH + "/" + cluster + "/" + entry + "/" + worker + "/" + assignedTask);
			}
		}
		
		zk.close();
		
		ZooKeeper zkov = ZooKeeperFactory.getLocalOverlayZooKeeper();
		if (zkov.exists(TsrConstants.ROOT_PATH, false) == null)
			return;
		
		for (String entry : new String[]{"data","entitle","patterns","status","tsconfig","eventconfig"}) {
			if (zkov.exists(TsrConstants.ROOT_PATH + "/" + cluster + "/" + entry, false) != null)
				for (String subPath : zkov.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/" + entry, false))
					dataService.deleteRecursiveOverlay(TsrConstants.ROOT_PATH + "/" + cluster + "/" + entry + "/" + subPath);
		}
		zkov.close();
	}
	
	protected static void configureSampleData() throws ParseException {
		//"ptrn_16_1000_100_8_1000"
		config1 = new TimeSeriesConfig("TestCluster", "EKG_Sample1", 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.DOUBLES), "ptrn_16_1000_100_8_1000", 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:00:05.399Z"));
		//"skchEX_1000_1_10"
		config2 = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1", 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), "skchEX_1000_1_10;ptrn_16_1_100_8_1", 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:00:59.999Z"));
		
		//use for 5 sample data configuration, inserts, & retrievals across each of the supported frequencies  
		freqConfigs.clear();

		freqConfigs.put(Frequency.MILLIS, new TimeSeriesConfig("TestCluster", "S_" + Frequency.MILLIS, 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
			DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
			DateTimeHelper.parseDateString("2014-11-22T00:00:00.004Z")));

		
		freqConfigs.put(Frequency.SECONDS, new TimeSeriesConfig("TestCluster", "S_" + Frequency.SECONDS, 0, Frequency.SECONDS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:00:04.000Z")));
		

		freqConfigs.put(Frequency.MINUTES, new TimeSeriesConfig("TestCluster", "S_" + Frequency.MINUTES, 0,  Frequency.MINUTES, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:04:00.000Z")));
		
		freqConfigs.put(Frequency.HOURS, new TimeSeriesConfig("TestCluster", "S_" + Frequency.HOURS, 0, Frequency.HOURS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T04:00:00.000Z")));
		
		freqConfigs.put(Frequency.DAYS, new TimeSeriesConfig("TestCluster", "S_" + Frequency.DAYS, 0, Frequency.DAYS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-26T00:00:00.000Z")));
		
		freqConfigs.put(Frequency.YEARS, new TimeSeriesConfig("TestCluster", "S_" + Frequency.YEARS, 0, Frequency.YEARS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2018-11-22T00:00:00.000Z")));
		
		freqConfigs.put(Frequency.MICROS, new TimeSeriesConfig("TestCluster", "S_" + Frequency.MICROS, 0, Frequency.MICROS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.233Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.233Z")));
		
		freqConfigs.put(Frequency.NANOS, new TimeSeriesConfig("TestCluster", "S_" + Frequency.NANOS, 0, Frequency.NANOS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.111Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.111Z")));

		
		//for (TimeSeriesConfig tsc : freqConfigs.values()) {
			//tsc.setRequestMaxStorage(false);
		//}
	}
	
	protected static void createMasterData() throws TimeSeriesException {
		//data tables created automatically when master data is added 
		IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
		ConfigurationProxy configProxy = new ConfigurationProxy(mdHandler, 0, config1.getCluster());
		configProxy.createConfig(config1);
		RESTFactory.getConfigurationProxy(addr).createConfigJSON(config1);
		RESTFactory.getConfigurationProxy(addr).createConfigJSON(config2);
		for (TimeSeriesConfig tsc: freqConfigs.values())
			RESTFactory.getConfigurationProxy(addr).createConfigJSON(tsc);
		mdHandler.createMasterData(config1);
		mdHandler.createMasterData(config2);
		for (TimeSeriesConfig tsc: freqConfigs.values())
			mdHandler.createMasterData(tsc);
	}
	
	protected static void indexSampleData() throws IOException, TimeSeriesException {
		int count = 10;
		//int count = 1 + 1 + freqConfigs.size() + 5;
		for (int i = 0; i < count ; i++) {
			SyncWorker sw = new SyncWorker();
			sw.setSpawner(false);
			if (sw.initialize())
				sw.register();
			
			IndexingWorker w1 = new IndexingWorker(IndexType.ptrn);
			w1.setSpawner(false);
			if (w1.initialize())
				w1.register();
			
			IndexingWorker w2 = new IndexingWorker(IndexType.skchEX);
			w2.setSpawner(false);
			if (w2.initialize())
				w2.register();
		}
		
		//for units tests on a single machine, run only one master since there is only one local ln
		IndexingMaster m = new IndexingMaster();
		if (m.initialize())
			m.runMasterTasks();
	}
	
	protected static void addSampleEKGData() throws TimeSeriesException, IOException {
		List<Double> data = getSampleEKGData();
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
		boolean inserted = (result.getErrorMessage() == null)?true:false;
		
		//boolean inserted = dataHandler.insertBulkData(serializedBytes);
		assert inserted;
	}
	
	protected static void addSampleZIPFData() throws IOException, TimeSeriesException {
		//Zipf dataGenerator = (new Zipf(1000,1.0));
		//(new Zipf(1000,1.0)).saveToFile(60000, "/Users/AgnitioWorks/Documents/workspace/TimeSeriesTranslator/ZIPF_Sample_trimmed.csv");
		//int[] samplestest = dataGenerator.sample(1000);
		List<Integer> samples = getSampleZipfData();
		Schema.Type timeSeriesValueType = Schema.Type.INT;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		for (int j = 0; j < samples.size(); j++)
			dataArray.add(handler.setDataRecord(j, new Integer(samples.get(j))));
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		blockArray.add(handler.setBlockRecord(config2.getCluster(), config2.getLogicalNode(), config2.getGuid(), config2.getStartDate().getMillis(), dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		
		TimeSeries ts = new TimeSeries();
		ts.setCluster(config2.getCluster());
		ts.setLn(config2.getLogicalNode());
		ts.setId(config2.getGuid());
		ts.setStart(config2.getStartDate().getMillis());
		ts.setEnd(config2.getStartDate().getMillis() + samples.size() - 1 );
		ts.setBulkData(RESTFactory.encode(serializedBytes));
		TimeSeries result = RESTFactory.getTimeSeriesProxy(addr).updateTimeSeriesBulkJSON(ts);
		boolean inserted = (result.getErrorMessage() == null)?true:false;
		//boolean inserted = dataHandler.insertBulkData(serializedBytes);
		assert inserted;
	}
	
	public static List<Double> getSampleEKGData() throws IOException {
		FileInputStream fis = new FileInputStream("EKG_Sample_trimmed.csv");
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		List<Double> data = new ArrayList<>();
		while ((line = br.readLine()) != null) {
			data.add(Double.parseDouble(line));
		}
		br.close();
		return data;
	}
	
	public static List<Integer> getSampleZipfData() throws IOException {
		FileInputStream fis = new FileInputStream("ZIPF_Sample_trimmed.csv");
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		List<Integer> data = new ArrayList<>();
		while ((line = br.readLine()) != null) {
			data.add(Integer.parseInt(line));
		}
		br.close();
		return data;
	}

	public static List<Match> getTopkMatches(SortedMap<Double,List<Match>> matches, int topk) {
		List<Match> topkMatches = new ArrayList<Match>();
		for (double dist : matches.keySet()) {
			for (Match match : matches.get(dist)) {
				if (topkMatches.size() < topk)
					topkMatches.add(match);
				else
					return topkMatches;
			}
		}
		return topkMatches;
	}
	
	public static double[] getSlice(int i, List<Integer> data, int dimension) {
		if (i > (data.size() - dimension))
			return null;
		double[] slice = new double[dimension];
		
		for (int j =0; j< dimension; j++)
			slice[j] = data.get(i+j);
		return slice;
	}
}
