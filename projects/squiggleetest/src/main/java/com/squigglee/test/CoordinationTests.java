// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.squigglee.coord.interfaces.IConfigService;
import com.squigglee.coord.interfaces.ICoordService;
import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.interfaces.IEntitlementService;
import com.squigglee.coord.interfaces.IIndexService;
import com.squigglee.coord.interfaces.ITaskService;
import com.squigglee.coord.utility.AuthenticationUtility;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.IndexingTask;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.View;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.interfaces.ISketchHandler;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;
import com.squigglee.core.sketch.CountExact;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.jobs.indexing.IndexingMaster;
import com.squigglee.jobs.indexing.IndexingWorker;
 
public class CoordinationTests {
	
	@org.junit.Test
	public void verifyCoordinationService() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//IHandlerFactory factory = new HandlerFactory();
		ICoordService coordService = ServiceFactory.getCoordinationService();
		ITaskService taskService = ServiceFactory.getTaskService();
		IEntitlementService entitlementService = ServiceFactory.getEntitlementService();
		IDataService dataService = ServiceFactory.getDataService();
		IConfigService configService = ServiceFactory.getConfigurationService();
		
		coordService.executeLine("ls /");
		coordService.executeLine("create /sometest \"\"");
		coordService.executeLine("deleteall /sometest");
		
		dataService.setNode("Cluster1", 0, "127.0.0.1", "local", "locahost", true, true, "TimeSeriesNode1", 0, 250, "Medium");
		
		TimeSeriesConfig config1 = new TimeSeriesConfig("Cluster1", "EKG_Sample1", 0, Frequency.MILLIS, 
				DynamicTypeTranslator.getViewDataType(View.DOUBLES), "ptrn_16_1000_100_8_1000", 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
		
		configService.createConfig(config1);
		
		TimeSeriesConfig config2 = new TimeSeriesConfig("Cluster1", "ZIPF_Sample1", 0, Frequency.MILLIS, 
				DynamicTypeTranslator.getViewDataType(View.INTEGERS), "skchEX_1000_1_10", 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
		
		configService.createConfig(config2);
		
		//statusService.setClusterStatus("Cluster1", 0, StatusType.STORAGE);
	
		//List<NodeStatus> clusterStatus = statusService.getClusterStatus("Cluster1");
		//assert(clusterStatus.get(0).isStorageServiceUp());
		
		IndexingTask newRequest = new IndexingTask("Cluster1", 0, 1L, 0, 1000, "ptrn_16_1000_100_8_1000", CommandType.INSERT);
		taskService.addTask(newRequest);
		
		//List<IndexingTask> pendingList = taskService.getUnassignedTasks();

		dataService.setNode("Cluster1", 3, "127.0.0.1", "local", "locahost", true, true, "TimeSeriesNode4", 0, 250, "Medium");
		
		TimeSeriesConfig config3 = new TimeSeriesConfig("Cluster1", "Test_Sample1", 3, Frequency.MILLIS, 
				DynamicTypeTranslator.getViewDataType(View.STRINGS), "", 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
		
		configService.createConfig(config3);
		
		//Map<String,List<TimeSeriesConfig>> node3Configuration = configService.getConfig("Cluster1",3);
		
		
		Map<String, Map<Integer, List<CommandType>>> defaultEntitlements0 = entitlementService.getEntitlements("Cluster1", 0);
		assertEquals(defaultEntitlements0.containsKey("Cluster1"), true);
		assertEquals(defaultEntitlements0.get("Cluster1").containsKey(0), true);
		assertEquals(defaultEntitlements0.get("Cluster1").get(0).contains(CommandType.SELECT), true);
		assertEquals(defaultEntitlements0.get("Cluster1").get(0).contains(CommandType.INSERT), true);
		assertEquals(defaultEntitlements0.get("Cluster1").get(0).contains(CommandType.UPDATE), true);
		assertEquals(defaultEntitlements0.get("Cluster1").get(0).contains(CommandType.DELETE), true);
		
		List<CommandType> perms = new ArrayList<CommandType>();
		perms.add(CommandType.SELECT);
		
		entitlementService.setEntitlement("Cluster1", 3, "Cluster1", 0, perms);
		perms.add(CommandType.INSERT);perms.add(CommandType.UPDATE);perms.add(CommandType.DELETE);
		entitlementService.setEntitlement("Cluster1", 0, "Cluster1", 3, perms);
		
		Map<String, Map<Integer, List<CommandType>>> node0Entitlements = entitlementService.getEntitlements("Cluster1", 0);
		Map<String, Map<Integer, List<CommandType>>> node3Entitlements = entitlementService.getEntitlements("Cluster1", 3);
		assertEquals(node0Entitlements.get("Cluster1").get(3).contains(CommandType.SELECT), true);
		assertEquals(node0Entitlements.get("Cluster1").get(3).contains(CommandType.INSERT), true);
		assertEquals(node0Entitlements.get("Cluster1").get(3).contains(CommandType.UPDATE), true);
		assertEquals(node0Entitlements.get("Cluster1").get(3).contains(CommandType.DELETE), true);
		assertEquals(node3Entitlements.get("Cluster1").get(0).contains(CommandType.SELECT), true);
		assertEquals(node3Entitlements.get("Cluster1").get(0).contains(CommandType.INSERT), false);
		assertEquals(node3Entitlements.get("Cluster1").get(0).contains(CommandType.UPDATE), false);
		assertEquals(node3Entitlements.get("Cluster1").get(0).contains(CommandType.DELETE), false);
		
		//cs.deleteNode("Cluster1", 0);
		//cs.deleteNode("Cluster1", 3);
		//cs.deleteCluster("Cluster1");
	}

	@org.junit.Test
	public void verifyAuthentication() throws Exception {
		String superpw = "super:zkadminpw";
		String digest = "super:qYTQ8gk6FODxaN7uX/1kXW0oZqY=";
		String computed = AuthenticationUtility.getDigestPassword(superpw);
		System.out.println(computed);
		assertEquals(digest,computed);
	}
	
	@org.junit.Test
	public void verifyStage1() throws Exception {
		String cipher = "BT JPX RMLX PCUV AMLX ICVJP IBTWXVR CI M LMT’R PMTN, MTN YVCJX CDXV MWMBTRJ JPX AMTNGXRJBAH UQCT JPX QGMRJXV CI JPX YMGG CI JPX HBTW’R QMGMAX; MTN JPX HBTW RMY JPX QMVJ CI JPX PMTN JPMJ YVCJX. JPXT JPX HBTW’R ACUTJXTMTAX YMR APMTWXN, MTN PBR JPCUWPJR JVCUFGXN PBL, RC JPMJ JPX SCBTJR CI PBR GCBTR YXVX GCCRXN, MTN PBR HTXXR RLCJX CTX MWMBTRJ MTCJPXV. JPX HBTW AVBXN MGCUN JC FVBTW BT JPX MRJVCGCWXVR, JPX APMGNXMTR, MTN JPX RCCJPRMEXVR. MTN JPX HBTW RQMHX, MTN RMBN JC JPX YBRX LXT CI FMFEGCT, YPCRCXDXV RPMGG VXMN JPBR YVBJBTW, MTN RPCY LX JPX BTJXVQVXJMJBCT JPXVXCI, RPMGG FX AGCJPXN YBJP RAM";
		
		StringBuffer output = new StringBuffer();
		SortedMap<String,String> key = new TreeMap<String,String>();
		key.put("A", "c");
		key.put("B", "i");
		key.put("C", "o");
		key.put("D", "v");
		key.put("E", "y");
		key.put("F", "b");
		key.put("G", "l");
		key.put("H", "k");
		key.put("I", "f");
		key.put("J", "t");
		key.put("K", "q");
		key.put("L", "m");
		key.put("M", "a");
		key.put("N", "d");
		key.put("O", "z");
		key.put("P", "h");
		key.put("Q", "p");
		key.put("R", "s");
		key.put("S", "j");
		key.put("T", "n");
		key.put("U", "u");
		key.put("V", "r");
		key.put("W", "g");
		key.put("X", "e");
		key.put("Y", "w");
		key.put("Z", "x");
		
		
		for (char cipherLetter : cipher.toCharArray()) {
			String letter = "" + cipherLetter;
			if (key.containsKey(letter))
				output.append(key.get(letter));
			else
				output.append(letter);
		}
		
		System.out.println(output.toString());
		
	}

	@org.junit.Test
	public void verifyPatternIndexing() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		IDataService dataService = ServiceFactory.getDataService();
		IDataHandler dataHandler = HandlerFactory.getDataHandler();
		IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
		String cluster = "TestCluster";
		dataService.executeLine("deleteall " + TsrConstants.ROOT_PATH + "/" + cluster);
		dataService.setNode("TestCluster", 0, "127.0.0.1", "local", "locahost", true, true, "TimeSeriesNode_0", 0, 250, "Medium");
		TimeSeriesConfig config0 = new TimeSeriesConfig("TestCluster", "TestParameter", 0, Frequency.MILLIS, 
				DynamicTypeTranslator.getViewDataType(View.INTEGERS), "", 
			DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
			DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
		//configService.createConfig("TestCluster", config0);
		TimeSeriesConfig config1 = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1", 0, Frequency.MILLIS, 
				DynamicTypeTranslator.getViewDataType(View.INTEGERS), "ptrn_24_1000_10_4_1", 
			DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
			DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
		//configService.createConfig("TestCluster", config1);
		mdHandler.createMasterData(config0);
		mdHandler.createMasterData(config1);
		
		/*
		FileInputStream fis = new FileInputStream("ZIPF_Sample_trimmed.csv");
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		List<Integer> samples = new ArrayList<>();
		while ((line = br.readLine()) != null) {
			samples.add(Integer.parseInt(line));
		}
		br.close();

		Schema.Type timeSeriesValueType = Schema.Type.INT;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		int sampleSize = samples.size();
		for (int j = 0; j < sampleSize; j++)
			dataArray.add(handler.setDataRecord(j, new Integer(samples.get(j))));
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		blockArray.add(handler.setBlockRecord(config1.getLogicalNode(), config1.getGuid(), config1.getStartDate().getMillis(), dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		int[] inserted = dataHandler.insertBulkData(serializedBytes);
		assertEquals(samples.size(), inserted[0]);
		*/
		IndexingWorker w = new IndexingWorker(IndexType.ptrn);
		w.initialize();
		w.register();
		
		IndexingMaster m = new IndexingMaster();
		if (m.initialize())
			m.runMasterTasks();
		
		dataHandler.shutdown();
		mdHandler.shutdown();
		
		System.in.read();
	}

	@org.junit.Test
	public void verifyIndexSerialization() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		IDataService dataService = ServiceFactory.getDataService();
		IIndexService indexService = ServiceFactory.getIndexService();
		IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();

		String cluster = "TestCluster";
		dataService.executeLine("deleteall " + TsrConstants.ROOT_PATH + "/" + cluster);
		dataService.setNode("TestCluster", 0, "127.0.0.1", "local", "locahost", true, true, "TimeSeriesNode_0", 0, 250, "Medium");
		//ptrn_24_1000_10_4_1
		TimeSeriesConfig config0 = new TimeSeriesConfig("TestCluster", "TestParameter", 0, Frequency.MILLIS, 
				DynamicTypeTranslator.getViewDataType(View.INTEGERS), "", 
			DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
			DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
		//configService.createConfig("TestCluster", config0);
		TimeSeriesConfig config1 = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1", 0, Frequency.MILLIS, 
				DynamicTypeTranslator.getViewDataType(View.INTEGERS), "skchEX_1000_1_10", 
			DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
			DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
		//configService.createConfig("TestCluster", config1);
		mdHandler.createMasterData(config0);
		mdHandler.createMasterData(config1);
		
		CountExact ce = new CountExact();
		ce.setValuesFromTableName("skchEX_1000_1_10_1");
		byte[] data0 = ce.serializeIndex();
		indexService.saveSerializedIndex("TestCluster",0, 1, "skchEX_1000_1_10", data0, true);
		
		ce.update(0, 100.0);
		ce.update(1, 50.0);
		ce.update(2, 100.0);
		ce.update(3, 100.0);
		assert ce.statistics().getCount() == 4;
		ce.reverseUpdate(3, 100.00);
		assert ce.statistics().getCount() == 3;
		
		assert ce.pointQuery(100.0) == 2;
		assert ce.pointQuery(50.0) == 1;
		assert ce.pointQuery(10.0) == 0;
		
		byte[] data = ce.serializeIndex();
		CountExact ce1 = new CountExact();
		ce1.deserializeIndex(data);
		
		assert ce1.pointQuery(100.0) == 2;
		assert ce1.pointQuery(50.0) == 1;
		assert ce1.pointQuery(10.0) == 0;
		
		indexService.saveSerializedIndex("TestCluster", 0, 1, "skchEX_1000_1_10", data, false);
		byte[] data2 = indexService.loadSerializedIndex("TestCluster", 0, 1, "skchEX_1000_1_10");
		assert data.length == data2.length;
		
		CountExact ce2 = new CountExact();
		ce2.deserializeIndex(data2);
		
		assert ce2.pointQuery(100.0) == 2;
		assert ce2.pointQuery(50.0) == 1;
		assert ce2.pointQuery(10.0) == 0;
		
		assert ce.getTableName().equals(ce1.getTableName());
		assert ce.getTableName().equals(ce2.getTableName());
		
		//coordService.executeLine("create /test");
		
		//System.in.read();
		//clean indexes as needed
		mdHandler.shutdown();
	}

	@org.junit.Test
	public void verifySketching() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		IDataService dataService = ServiceFactory.getDataService();
		IDataHandler dataHandler = HandlerFactory.getDataHandler();
		IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
		ISketchHandler sketchHandler = HandlerFactory.getSketchHandler();
		String cluster = "TestCluster";
		
		dataHandler.deleteData("TestCluster", 0, "ZIPF_Sample1",  DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z").getMillis(), 0, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:59.999Z").getMillis(), 0);
		
		
		dataService.executeLine("deleteall " + TsrConstants.ROOT_PATH + "/" + cluster);
		dataService.setNode("TestCluster", 0, "127.0.0.1", "local", "locahost", true, true, "TimeSeriesNode_0", 0, 250, "Medium");
		//ptrn_24_1000_10_4_1
		//skchEX_1000_1_10
		TimeSeriesConfig config0 = new TimeSeriesConfig("TestCluster", "TestParameter", 0, Frequency.MILLIS, 
				DynamicTypeTranslator.getViewDataType(View.INTEGERS), "", 
			DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
			DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
		//configService.createConfig("TestCluster", config0);
		TimeSeriesConfig config1 = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1", 0,  Frequency.MILLIS, 
				DynamicTypeTranslator.getViewDataType(View.INTEGERS), "skchEX_1000_1_10", 
			DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
			DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
		//configService.createConfig("TestCluster", config1);
		mdHandler.createMasterData(config0);
		mdHandler.createMasterData(config1);
		//mdHandler.deleteMasterData(config0);
		
		
		FileInputStream fis = new FileInputStream("ZIPF_Sample_trimmed.csv");
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		List<Integer> samples = new ArrayList<>();
		while ((line = br.readLine()) != null) {
			samples.add(Integer.parseInt(line));
		}
		br.close();

		Schema.Type timeSeriesValueType = Schema.Type.INT;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		int sampleSize = samples.size();
		for (int j = 0; j < sampleSize; j++)
			dataArray.add(handler.setDataRecord(j, new Integer(samples.get(j))));
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		blockArray.add(handler.setBlockRecord(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), config1.getStartDate().getMillis(), dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		boolean resultStatus = dataHandler.insertBulkData(serializedBytes);
		assert resultStatus;
		
		IndexingWorker w2 = new IndexingWorker(IndexType.skchEX);
		w2.initialize();
		w2.register();
		
		IndexingMaster m = new IndexingMaster();
		if (m.initialize())
			m.runMasterTasks();
		
		System.in.read();
		
		assert sketchHandler.statistics("TestCluster", 0, "ZIPF_Sample1").getCount() == 60000;
		
		dataHandler.deleteData("TestCluster", 0, "ZIPF_Sample1", DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z").getMillis(), 0, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.001Z").getMillis(), 0);
		System.in.read();
		
		assert sketchHandler.statistics("TestCluster", 0, "ZIPF_Sample1").getCount() == 59998;
		System.in.read();
		
		//cleanup
		dataHandler.shutdown();
		mdHandler.shutdown();
		sketchHandler.shutdown();
	}

	@org.junit.Test
	public void verifyPostingJobs() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		ICoordService coordService = ServiceFactory.getCoordinationService();
		ITaskService taskService = ServiceFactory.getTaskService();
		IDataService dataService = ServiceFactory.getDataService();
		IConfigService configService = ServiceFactory.getConfigurationService();
		
		IDataHandler dataHandler = HandlerFactory.getDataHandler();
		IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
		ISketchHandler sketchHandler = HandlerFactory.getSketchHandler();
		String cluster = "TestCluster";
		TimeSeriesConfig config0 = new TimeSeriesConfig("TestCluster", "TestParameter", 0, Frequency.MILLIS, 
				DynamicTypeTranslator.getViewDataType(View.INTEGERS), "", 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
			TimeSeriesConfig config1 = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1", 0, Frequency.MILLIS, 
					DynamicTypeTranslator.getViewDataType(View.INTEGERS), "skchEX_1000_1_10", 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z"), 
				DateTimeHelper.parseDateString("2014-11-22T23:59:59.999Z"));
		
		FileInputStream fis = new FileInputStream("ZIPF_Sample_trimmed.csv");
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		List<Integer> samples = new ArrayList<>();
		while ((line = br.readLine()) != null) {
			samples.add(Integer.parseInt(line));
		}
		br.close();
	
		Schema.Type timeSeriesValueType = Schema.Type.INT;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		int sampleSize = samples.size();
		for (int j = 0; j < sampleSize; j++)
			dataArray.add(handler.setDataRecord(j, new Integer(samples.get(j))));
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		blockArray.add(handler.setBlockRecord(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), config1.getStartDate().getMillis(), dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		boolean resultStatus = dataHandler.insertBulkData(serializedBytes);
		assert resultStatus;
		
		dataService.executeLine("deleteall " + TsrConstants.ROOT_PATH + "/" + cluster);
		dataService.setNode(cluster, 0, "127.0.0.1", "local", "locahost", true, true, "TimeSeriesNode_0", 0, 250, "Medium");
		//ptrn_24_1000_10_4_1
		//skchEX_1000_1_10
		mdHandler.createMasterData(config0);
		mdHandler.createMasterData(config1);
		//mdHandler.deleteMasterData(config0);
		
		IndexingWorker w2 = new IndexingWorker(IndexType.skchEX);
		w2.initialize();
		w2.register();
		
		IndexingMaster m = new IndexingMaster();
		if (m.initialize())
			m.runMasterTasks();
		
		//System.in.read();
		
		assert sketchHandler.statistics("TestCluster", 0, "ZIPF_Sample1").getCount() == 60000;
		
		dataHandler.deleteData("TestCluster", 0, "ZIPF_Sample1", DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z").getMillis(), 0, 
				DateTimeHelper.parseDateString("2014-11-22T00:00:00.001Z").getMillis(), 0);
		
		//System.in.read();
		
		assert sketchHandler.statistics("TestCluster", 0, "ZIPF_Sample1").getCount() == 59998;
		//System.in.read();
		
		//cleanup
		coordService.close();
		dataService.close();
		//statusService.close();
		configService.close();
		//entitlementService.close();
		taskService.close();
		dataHandler.shutdown();
		mdHandler.shutdown();
		sketchHandler.shutdown();
	}
}