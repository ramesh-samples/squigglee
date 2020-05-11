// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

//import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.zookeeper.ZooKeeper;
import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.coord.interfaces.ICEPService;
import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.coord.utility.ZooKeeperFactory;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;
import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.Query;
import com.squigglee.core.entity.Stream;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.interfaces.ICEPDataHandler;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.jobs.event.EventWorker;
import com.squigglee.jobs.sync.SyncWorker;

public class TestBaseCEP {
	
	protected static TestUtility utility = null;
	protected static TimeSeriesConfig config = null;
	protected static TimeSeriesConfig config_SECONDS = null;
	protected static TimeSeriesConfig config_MINUTES = null;
	protected static TimeSeriesConfig config_HOURS = null;
	protected static TimeSeriesConfig config_transformed = null;
	protected static String cluster = null;
	protected static int ln = 0;
	protected static String addr = null;
	protected static Stream stream1 = null;
	protected static Stream stream2 = null;
	protected static Stream stream3 = null;
	protected static Stream stream4 = null;
	protected static ICEPDataHandler cepHandler = null;
	
	public static void setUpEnvironment() throws TimeSeriesException {
		utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		HandlerFactory.initialize("com.squigglee.storage.mbb");
		cepHandler = HandlerFactory.getCEPDataHandler();
		//RESTFactory.initialize("/squiggleerestui", 8080, "http", 20000, 20000);
		cluster = LocalNodeProperties.getClusterName();
		ln = LocalNodeProperties.getNodeLogicalNumber();
		addr = "127.0.0.1";
	}
	
	public static void setUpHandlers() throws TimeSeriesException {
	}
	
	public static void setUpStorage() throws Exception {
		IDataService dataService = com.squigglee.coord.utility.ServiceFactory.getDataService();
		//dataService.deleteNode(cluster, ln);
		//dataService.deleteNode(cluster, ln);
		//dataService.deleteCluster(cluster);
		cleanData(dataService);
		dataService.setupCluster(cluster);
		//RESTFactory.getConfigurationProxy(addr).setupCluster(cluster);
		//configRestClient.setupCluster(cluster);
		dataService.setNode(cluster, ln, "127.0.0.1", "local", "local",	true, true, "TimeSeriesNode_0", ln, 100, "Medium");
		//RESTFactory.getConfigurationProxy(addr).updateNode(cluster, ln, "127.0.0.1", "local", "local", "TimeSeriesNode_0", true, true, ln, 100, "Medium");
		//statusHandler.updateNode(ln, "127.0.0.1", "local", "local", "TimeSeriesNode_0", true, true, ln, 100, "Medium");
		
		configureSampleData();
		createMasterData();
		Thread.sleep(2000);
		setupCEPData();
		Thread.sleep(2000);
		indexSampleData();
		Thread.sleep(2000);
		addSampleZIPFData();
		Thread.sleep(2000);
		System.in.read();
	}
	
	public static void cleanUpStorage() throws TimeSeriesException {
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
		
		for (String entry : new String[]{"indexes","events","syncqueue","syncs","tasks","taskqueue"}) {
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
		config = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1", 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:00:59.999Z"));
		//auto roll-up example 
		config_SECONDS = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1_SECONDS", 0, Frequency.SECONDS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:00:59.999Z"), true);
		
		config_MINUTES = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1_MINUTES", 0, Frequency.MINUTES, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:00:59.999Z"), true);
		
		config_HOURS = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1_HOURS", 0, Frequency.HOURS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:00:59.999Z"), true);
		
		config_transformed = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1_Transformed", 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), null, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T00:00:59.999Z"));
	}
	
	protected static void createMasterData() throws TimeSeriesException {
		//data tables created automatically when master data is added 
		IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
		//ConfigurationProxy configProxy = new ConfigurationProxy(mdHandler, 0, config.getCluster());
		//configProxy.createConfig(config);
		//RESTFactory.getConfigurationProxy(addr).createConfigJSON(config);
		mdHandler.createMasterData(config);
		mdHandler.createMasterData(config_SECONDS);
		mdHandler.createMasterData(config_MINUTES);
		mdHandler.createMasterData(config_HOURS);
		mdHandler.createMasterData(config_transformed);
	}
	
	protected static void setupCEPData() throws TimeSeriesException {
		ICEPService cepService = ServiceFactory.getCEPService();
		stream1 = new Stream(UUID.randomUUID().toString(), "TestCluster", 0, config.getGuid(), false, false, false);
		stream2 = new Stream(UUID.randomUUID().toString(), "TestCluster", 0, "Threshold_995_Stream", true, false, false);
		stream3 = new Stream(UUID.randomUUID().toString(), "TestCluster", 0, "Threshold_998_Stream", true, true, false);
		stream4 = new Stream(UUID.randomUUID().toString(), "TestCluster", 0, config_transformed.getGuid(), true, false, true);
		cepService.addStream("TestCluster", 0, stream1);
		cepService.addStream("TestCluster", 0, stream2);
		cepService.addStream("TestCluster", 0, stream3);
		cepService.addStream("TestCluster", 0, stream4);
		
		Query query1 = new Query(UUID.randomUUID().toString(), "TestCluster", 0, "Threshold_995_Query",
				"from " + stream1.getId() + "[value>995] insert into " + stream2.getId());
		Query query2 = new Query(UUID.randomUUID().toString(), "TestCluster", 0, "Threshold_998_Query",
				"from " + stream2.getId() + "[value>998] insert into " + stream3.getId());
		Query query3 = new Query(UUID.randomUUID().toString(), "TestCluster", 0, "Transform_Query",
				"from " + stream1.getId() + " select startts, offset, value*value as value insert into " + stream4.getId() + " ;");
		
		cepService.addQuery("TestCluster", 0, query1);
		cepService.addQuery("TestCluster", 0, query2);
		cepService.addQuery("TestCluster", 0, query3);
	}
	
	protected static void indexSampleData() throws IOException, TimeSeriesException {
		SyncWorker sw = new SyncWorker();
		sw.setSpawner(false);
		if (sw.initialize())
			sw.register();
		
		EventWorker cepw = new EventWorker();
		if (cepw.initialize())
			cepw.register();
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
		blockArray.add(handler.setBlockRecord(config.getCluster(), config.getLogicalNode(), config.getGuid(), config.getStartDate().getMillis(), dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		
		TimeSeries ts = new TimeSeries();
		ts.setCluster(config.getCluster());
		ts.setLn(config.getLogicalNode());
		ts.setId(config.getGuid());
		ts.setStart(config.getStartDate().getMillis());
		ts.setEnd(config.getStartDate().getMillis() + samples.size() - 1 );
		ts.setBulkData(RESTFactory.encode(serializedBytes));
		//TimeSeries result = RESTFactory.getTimeSeriesProxy(addr).updateTimeSeriesBulkJSON(ts);
		//boolean inserted = (result.getErrorMessage() == null)?true:false;
		boolean inserted = cepHandler.insertBulkData(serializedBytes);
		assert inserted;
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
}
