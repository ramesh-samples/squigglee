// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

//import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.zookeeper.ZooKeeper;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;

import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.coord.utility.ZooKeeperFactory;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;
import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.Match;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.jobs.indexing.IndexingMaster;
import com.squigglee.jobs.indexing.IndexingWorker;
import com.squigglee.jobs.sync.SyncWorker;
import com.squigglee.adapter.TimeSeriesTranslator;

/**
 * Remember to bootstrap master data i.e. create "ks_md" keyspace & create the "md" table in that keyspace
 * @author AgnitioWorks
 *
 */
public class TestBase {
	
	protected static JdbcDataSource h2ds1 = null;
	protected static JdbcDataSource h2ds2 = null;
	protected static String url1 = null;
	protected static String url2 = null;
	protected static EmbeddedServer.ConnectionFactoryProvider<DataSource> h2JdbcProvider1 = null;
	protected static EmbeddedServer.ConnectionFactoryProvider<DataSource> h2JdbcProvider2 = null;
	protected static Connection srcCon1 = null; 
	protected static Connection srcCon2 = null; 
	protected static EmbeddedConfiguration ec = null;
	protected static EmbeddedServer teiidServer = null; 
	protected static ModelMetaData sourceModel1 = null;
	protected static ModelMetaData sourceModel2 = null;
	protected static ModelMetaData viewModel1 = null;
	protected static ModelMetaData viewModel2 = null;
	protected static ModelMetaData globalViewModel = null;
	protected static TeiidDriver td = null;
	protected static Connection viewCon1 = null;
	protected static Connection viewCon2 = null;
	protected static Connection globalViewCon = null;
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
	
	public static void setUpEnvironment() throws TimeSeriesException {
		utility = new TestUtility();
		//utility.setEnvProps("Test Cluster",9160,"127.0.0.1:9160", 0, 60000, 10, -1, TranslatorConstants.THRIFT_MODE,
		//		TranslatorConstants.THRIFT_MODE, TranslatorConstants.HANDLER_SERIALIZER_AVRO, 120, "/Users/AgnitioWorks/Documents/tsr/ansible/TIMESERIESGLOBAL-vdb.xml");
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//serviceFactory = new ServiceFactory();
		cluster = LocalNodeProperties.getClusterName();
		ln = LocalNodeProperties.getNodeLogicalNumber();
	}
	
	public static void setUpHandlers() throws TimeSeriesException {			
	}
	
	public static void setUpStorage() throws Exception {
		IDataService dataService = ServiceFactory.getDataService();
		//dataService.deleteNode(cluster, ln);
		//statusHandler.deleteNode(ln);
		//dataService.deleteCluster(cluster);
		cleanData(dataService);
		dataService.setupCluster(cluster);
		dataService.setNode(cluster, ln, "127.0.0.1", "local", "local",	true, true, "TimeSeriesNode_0", ln, 100, "Medium");
		//statusHandler.updateNode(ln, "127.0.0.1", "local", "local", "TimeSeriesNode_0", true, true, ln, 100, "Medium");
		
		configureSampleData();
		createMasterData();
		//Thread.sleep(10000);
		//indexSampleData();
		//Thread.sleep(10000);
		//addSampleEKGData();
		//Thread.sleep(20000);
		//addSampleZIPFData();
		//Thread.sleep(20000);
	}
	
	public static void cleanUpStorage() throws TimeSeriesException {
		//
		
		
		//get all the master records for the test config1 
		List<MasterData> mdList1 = HandlerFactory.getMasterDataHandler().getMasterData(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), 
				config1.getStartDate().getMillis(), config1.getEndDate().getMillis());
		//drop the test data tables 
		for (MasterData md : mdList1) {
			HandlerFactory.getIndexHandler().updateIndex(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), config1.getIndexes(), true);
			HandlerFactory.getSchemaHandler().deleteSchema(md.getCluster(), md.getId());
		}

		//get all the master records for the test config2 
		List<MasterData> mdList2 = HandlerFactory.getMasterDataHandler().getMasterData(config2.getCluster(), config2.getLogicalNode(), config2.getGuid(), 
				config2.getStartDate().getMillis(), config2.getEndDate().getMillis());
		//drop the test data tables 
		for (MasterData md : mdList2) {
			HandlerFactory.getIndexHandler().updateIndex(config2.getCluster(), config2.getLogicalNode(), config2.getGuid(), config2.getIndexes(), true);
			HandlerFactory.getSchemaHandler().deleteSchema(md.getCluster(), md.getId());
		}
		
		for (TimeSeriesConfig config : freqConfigs.values()) {
			
			List<MasterData> mdList = HandlerFactory.getMasterDataHandler().getMasterData(config.getCluster(), config.getLogicalNode(), config.getGuid(), 
					config.getStartDate().getMillis(), config.getEndDate().getMillis());
			for (MasterData md : mdList) {
				HandlerFactory.getIndexHandler().updateIndex(config.getCluster(), config.getLogicalNode(), config.getGuid(), config.getIndexes(), true);
				HandlerFactory.getSchemaHandler().deleteSchema(md.getCluster(), md.getId());
			}
		}
		
		//dataService.deleteNode(cluster, ln);
		//dataService.deleteCluster(cluster);
	}
	
	public static void setUpSources() throws Exception {
		h2ds1 = new JdbcDataSource();
		url1 = "jdbc:h2:mem:Cassandra";
		h2ds1.setURL(url1);
		h2ds1.setUser("CASSANDRA");
		h2ds1.setPassword("CASSANDRA");
		h2JdbcProvider1 = new EmbeddedServer.SimpleConnectionFactoryProvider<DataSource>(h2ds1);		
		srcCon1 = h2JdbcProvider1.getConnectionFactory().getConnection();
		
		execute(srcCon1, "DROP SCHEMA IF EXISTS TIMESERIES;", false);
		execute(srcCon1, "CREATE SCHEMA TIMESERIES;", false);
		execute(srcCon1, "CREATE TABLE TIMESERIES.DOUBLES (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, val double, CONSTRAINT DOUBLES_PK PRIMARY KEY (ln,id,ts));", false);
		execute(srcCon1, "CREATE TABLE TIMESERIES.SAMPLEDDOUBLES (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, val double, sf double not null);", false);
		execute(srcCon1, "CREATE TABLE TIMESERIES.MASTERDATA (id VARCHAR(128) NOT NULL, logicalnode integer NOT NULL, replication VARCHAR(512), strategy VARCHAR(128), frequency VARCHAR(128), datatype varchar(128), indexes varchar(4096), storagestart TIMESTAMP NOT NULL, storageend TIMESTAMP NOT NULL, CONSTRAINT MD_PK PRIMARY KEY (logicalnode,id))", false);
		execute(srcCon1, "CREATE TABLE TIMESERIES.PATTERNDOUBLES (pguid VARCHAR(128) not null, pindx INTEGER NOT NULL, val DOUBLE);", false);
		execute(srcCon1, "CREATE TABLE TIMESERIES.INTEGERS (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val integer);", false);
		execute(srcCon1, "CREATE TABLE TIMESERIES.SKETCHEDINTEGERS (ln integer not null, id VARCHAR(128) NOT NULL, freq double not null, val integer not null, st varchar(128) not null);", false);
		execute(srcCon1, "CREATE TABLE TIMESERIES.MATCHEDDOUBLES (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, val double, pid varchar(128) not null, rank integer not null, radius double not null);", false);
		execute(srcCon1, "CREATE TABLE TIMESERIES.BULKDATA (ln integer not null, id VARCHAR(128) NOT NULL, startdt timestamp, startoffset integer, enddt timestamp, endoffset integer, datatype VARCHAR(128), bdata blob);", false);
		
		sourceModel1 = new ModelMetaData();
		sourceModel1.setName("CassandraSourceModel1");
		sourceModel1.setSchemaSourceType("native");
		sourceModel1.addSourceMapping("TIMESERIES1", "squiggleeadapter", "cassandraclustersourcejndi1");
		
		h2ds2 = new JdbcDataSource();
		url2 = "jdbc:h2:mem:Cassandra";
		h2ds2.setURL(url2);
		h2ds2.setUser("CASSANDRA");
		h2ds2.setPassword("CASSANDRA");
		h2JdbcProvider2 = new EmbeddedServer.SimpleConnectionFactoryProvider<DataSource>(h2ds2);		
		srcCon2 = h2JdbcProvider2.getConnectionFactory().getConnection();
		
		execute(srcCon2, "DROP SCHEMA IF EXISTS TIMESERIES;", false);
		execute(srcCon2, "CREATE SCHEMA TIMESERIES;", false);
		execute(srcCon2, "CREATE TABLE TIMESERIES.DOUBLES (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double);", false);
		execute(srcCon2, "CREATE TABLE TIMESERIES.SAMPLEDDOUBLES (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double, sf double not null);", false);
		execute(srcCon2, "CREATE TABLE TIMESERIES.MASTERDATA (id VARCHAR(128) NOT NULL, logicalnode integer NOT NULL, replication VARCHAR(512), strategy VARCHAR(128), frequency VARCHAR(128), datatype varchar(128), indexes varchar(4096), storagestart TIMESTAMP NOT NULL, storageend TIMESTAMP NOT NULL, CONSTRAINT MD_PK PRIMARY KEY (logicalnode,id))", false);
		execute(srcCon2, "CREATE TABLE TIMESERIES.PATTERNDOUBLES (pguid VARCHAR(128) not null, pindx INTEGER NOT NULL, val DOUBLE);", false);
		execute(srcCon2, "CREATE TABLE TIMESERIES.INTEGERS (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val integer);", false);
		execute(srcCon2, "CREATE TABLE TIMESERIES.SKETCHEDINTEGERS (ln integer not null, id VARCHAR(128) NOT NULL, freq double not null, val integer not null, st varchar(128) not null);", false);
		execute(srcCon2, "CREATE TABLE TIMESERIES.MATCHEDDOUBLES (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double, pid varchar(128) not null, rank integer not null, radius double not null);", false);
		execute(srcCon2, "CREATE TABLE TIMESERIES.BULKDATA (ln integer not null, id VARCHAR(128) NOT NULL, startdt timestamp, startoffset integer, enddt timestamp, endoffset integer, datatype VARCHAR(128), bdata blob);", false);
	
		sourceModel2 = new ModelMetaData();
		sourceModel2.setName("CassandraSourceModel2");
		sourceModel2.setSchemaSourceType("native");
		sourceModel2.addSourceMapping("TIMESERIES2", "squiggleeadapter", "cassandraclustersourcejndi2");
	}

	public static void setUpViews() {
		viewModel1 = new ModelMetaData();
		viewModel1.setName("TimeSeriesViewModel");
		viewModel1.setModelType(Type.VIRTUAL);
		viewModel1.setSchemaSourceType("ddl");

		String ddlSchemaText1 = " CREATE VIEW \"Doubles\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.ln, A.id, A.ts, A.off, A.val FROM CassandraSourceModel1.CASSANDRA.TIMESERIES.DOUBLES A;  \r\n";
		ddlSchemaText1 += " CREATE VIEW \"SampledDoubles\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double, sf double not null)  OPTIONS (UPDATABLE 'FALSE') AS SELECT A.ln, A.id, A.ts, A.off, A.val, A.sf FROM CassandraSourceModel1.CASSANDRA.TIMESERIES.SAMPLEDDOUBLES A;  \r\n";
		ddlSchemaText1 += " CREATE VIEW \"MasterData\" (ID VARCHAR(128) NOT NULL, LOGICALNODE integer NOT NULL, REPLICATION VARCHAR(512), STRATEGY VARCHAR(128), FREQUENCY VARCHAR(128), DATATYPE varchar(128), indexes varchar(4096), STORAGESTART TIMESTAMP NOT NULL, STORAGEEND TIMESTAMP NOT NULL, CONSTRAINT MD_PK PRIMARY KEY (LOGICALNODE,ID)) OPTIONS (UPDATABLE 'TRUE') AS SELECT A.id, A.logicalnode, A.replication, A.strategy, A.frequency, A.datatype, A.indexes, A.storagestart, A.storageend FROM CassandraSourceModel1.CASSANDRA.TIMESERIES.MASTERDATA A;";
		ddlSchemaText1 += " CREATE VIEW \"PatternDoubles\" (pguid VARCHAR(128) NOT NULL, pindx integer NOT NULL, val double)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.pguid, A.pindx, A.val FROM CassandraSourceModel1.CASSANDRA.TIMESERIES.PATTERNDOUBLES A;  \r\n";
		ddlSchemaText1 += " CREATE VIEW \"Integers\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val integer)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.ln, A.id, A.ts, A.off, A.val FROM CassandraSourceModel1.CASSANDRA.TIMESERIES.INTEGERS A;  \r\n";
		ddlSchemaText1 += " CREATE VIEW \"SketchedIntegers\" (ln integer not null, id VARCHAR(128) NOT NULL, freq double not null, val integer, st varchar(128) not null)  OPTIONS (UPDATABLE 'FALSE') AS SELECT A.ln, A.id, A.freq, A.val, A.st FROM CassandraSourceModel1.CASSANDRA.TIMESERIES.SKETCHEDINTEGERS A;  \r\n";
		ddlSchemaText1 += " CREATE VIEW \"MatchedDoubles\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double, pid varchar(128) not null, rank integer not null, radius double not null)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.ln, A.id, A.ts, A.off, A.val, A.pid, A.rank, A.radius FROM CassandraSourceModel1.CASSANDRA.TIMESERIES.MatchedDoubles A;  \r\n";
		ddlSchemaText1 += " CREATE VIEW \"BulkData\" (ln integer not null, id VARCHAR(128) NOT NULL, startdt timestamp, startoffset integer, enddt timestamp, endoffset integer, datatype VARCHAR(128), bdata blob)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.ln, A.id, A.startdt, A.startoffset, A.enddt, A.endoffset, A.datatype, A.bdata FROM \"CassandraSourceModel1\".CASSANDRA.TIMESERIES.BULKDATA A;  \r\n";
		
		viewModel1.setSchemaText(ddlSchemaText1);
		
		viewModel2 = new ModelMetaData();
		viewModel2.setName("TimeSeriesViewModel3");
		viewModel2.setModelType(Type.VIRTUAL);
		viewModel2.setSchemaSourceType("ddl");
		
		String ddlSchemaText2 = " CREATE VIEW \"Doubles\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.ln, A.id, A.ts, A.off, A.val FROM CassandraSourceModel2.CASSANDRA.TIMESERIES.DOUBLES A;  \r\n";
		ddlSchemaText2 += " CREATE VIEW \"SampledDoubles\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double, sf double not null)  OPTIONS (UPDATABLE 'FALSE') AS SELECT A.ln, A.id, A.ts, A.off, A.val, A.sf FROM CassandraSourceModel2.CASSANDRA.TIMESERIES.SAMPLEDDOUBLES A;  \r\n";
		ddlSchemaText2 += " CREATE VIEW \"MasterData\" (ID VARCHAR(128) NOT NULL, LOGICALNODE integer NOT NULL, REPLICATION VARCHAR(512), STRATEGY VARCHAR(128), FREQUENCY VARCHAR(128), DATATYPE varchar(128), indexes varchar(4096), STORAGESTART TIMESTAMP NOT NULL, STORAGEEND TIMESTAMP NOT NULL, CONSTRAINT MD_PK PRIMARY KEY (LOGICALNODE,ID)) OPTIONS (UPDATABLE 'TRUE') AS SELECT A.id, A.logicalnode, A.replication, A.strategy, A.frequency, A.datatype, A.indexes, A.storagestart, A.storageend FROM CassandraSourceModel2.CASSANDRA.TIMESERIES.MASTERDATA A;";
		ddlSchemaText2 += " CREATE VIEW \"PatternDoubles\" (pguid VARCHAR(128) NOT NULL, pindx integer NOT NULL, val double)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.pguid, A.pindx, A.val FROM CassandraSourceModel2.CASSANDRA.TIMESERIES.PATTERNDOUBLES A;  \r\n";
		ddlSchemaText2 += " CREATE VIEW \"Integers\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val integer)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.ln, A.id, A.ts, A.off, A.val FROM CassandraSourceModel2.CASSANDRA.TIMESERIES.INTEGERS A;  \r\n";
		ddlSchemaText2 += " CREATE VIEW \"SketchedIntegers\" (ln integer not null, id VARCHAR(128) NOT NULL, freq double not null, val integer, st varchar(128) not null)  OPTIONS (UPDATABLE 'FALSE') AS SELECT A.ln, A.id, A.freq, A.val, A.st FROM CassandraSourceModel2.CASSANDRA.TIMESERIES.SKETCHEDINTEGERS A;  \r\n";
		ddlSchemaText2 += " CREATE VIEW \"MatchedDoubles\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double, pid varchar(128) not null, rank integer not null, radius double not null)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.ln, A.id, A.ts, A.off, A.val, A.pid, A.rank, A.radius FROM CassandraSourceModel2.CASSANDRA.TIMESERIES.MatchedDoubles A;  \r\n";
		ddlSchemaText2 += " CREATE VIEW \"BulkData\" (ln integer not null, id VARCHAR(128) NOT NULL, startdt timestamp, startoffset integer, enddt timestamp, endoffset integer, datatype VARCHAR(128), bdata blob)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.ln, A.id, A.startdt, A.startoffset, A.enddt, A.endoffset, A.datatype, A.bdata FROM \"CassandraSourceModel2\".CASSANDRA.TIMESERIES.BULKDATA A;  \r\n";
		
		viewModel2.setSchemaText(ddlSchemaText2);
		
		globalViewModel = new ModelMetaData();
		globalViewModel.setName("TimeSeriesGlobalViewModel");
		globalViewModel.setModelType(Type.VIRTUAL);
		globalViewModel.setSchemaSourceType("ddl");
		
		String ddlSchemaTextGlobal = " CREATE VIEW \"Doubles\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double)  OPTIONS (UPDATABLE 'FALSE') AS SELECT A0.ln, A0.id, A0.ts, A0.off, A0.val FROM TimeSeriesViewModel.DOUBLES A0 WHERE A0.ln in (0) UNION ALL SELECT A3.ln, A3.id, A3.ts, A3.off, A3.val FROM TimeSeriesViewModel3.DOUBLES A3 WHERE A3.ln in (3);  \r\n";
		ddlSchemaTextGlobal += " CREATE VIEW \"SampledDoubles\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double, sf double not null)  OPTIONS (UPDATABLE 'FALSE') AS SELECT A.ln, A.id, A.ts, A.off, A.val, A.sf FROM TimeSeriesViewModel.SAMPLEDDOUBLES A;  \r\n";
		ddlSchemaTextGlobal += " CREATE VIEW \"MasterData\" (ID VARCHAR(128) NOT NULL, LOGICALNODE integer NOT NULL, REPLICATION VARCHAR(512), STRATEGY VARCHAR(128), FREQUENCY VARCHAR(128), DATATYPE varchar(128), indexes varchar(4096), STORAGESTART TIMESTAMP NOT NULL, STORAGEEND TIMESTAMP NOT NULL, CONSTRAINT MD_PK PRIMARY KEY (LOGICALNODE,ID)) OPTIONS (UPDATABLE 'TRUE') AS SELECT A.id, A.logicalnode, A.replication, A.strategy, A.frequency, A.datatype, A.indexes, A.storagestart, A.storageend FROM TimeSeriesViewModel.MASTERDATA A;";
		ddlSchemaTextGlobal += " CREATE VIEW \"PatternDoubles\" (pguid VARCHAR(128) NOT NULL, pindx integer NOT NULL, val double)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.pguid, A.pindx, A.val FROM TimeSeriesViewModel.PATTERNDOUBLES A;  \r\n";
		ddlSchemaTextGlobal += " CREATE VIEW \"Integers\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val integer)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.ln, A.id, A.ts, A.off, A.val FROM TimeSeriesViewModel.INTEGERS A;  \r\n";
		ddlSchemaTextGlobal += " CREATE VIEW \"SketchedIntegers\" (ln integer not null, id VARCHAR(128) NOT NULL, freq double not null, val integer, st varchar(128) not null)  OPTIONS (UPDATABLE 'FALSE') AS SELECT A.ln, A.id, A.freq, A.val, A.st FROM TimeSeriesViewModel.SKETCHEDINTEGERS A;  \r\n";
		ddlSchemaTextGlobal += " CREATE VIEW \"MatchedDoubles\" (ln integer not null, id VARCHAR(128) NOT NULL, ts TIMESTAMP NOT NULL, off integer, val double, pid varchar(128) not null, rank integer not null, radius double not null)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A.ln, A.id, A.ts, A.off, A.val, A.pid, A.rank, A.radius FROM TimeSeriesViewModel.MatchedDoubles A;  \r\n";
		ddlSchemaTextGlobal += " CREATE VIEW \"BulkData\" (ln integer not null, id VARCHAR(128) NOT NULL, startdt timestamp, startoffset integer, enddt timestamp, endoffset integer, datatype VARCHAR(128), bdata blob)  OPTIONS (UPDATABLE 'TRUE') AS SELECT A0.ln, A0.id, A0.startdt, A0.startoffset, A0.enddt, A0.endoffset, A0.datatype, A0.bdata FROM TimeSeriesViewModel.BULKDATA A0 UNION ALL SELECT A3.ln, A3.id, A3.startdt, A3.startoffset, A3.enddt, A3.endoffset, A3.datatype, A3.bdata FROM TimeSeriesViewModel3.BULKDATA A3;  \r\n";
		
		globalViewModel.setSchemaText(ddlSchemaTextGlobal);
	}
	
	@BeforeClass
	public static void setUpClass() throws Exception {
		setUpEnvironment();
		setUpHandlers();
		setUpStorage();

		setUpSources();
		ec = new EmbeddedConfiguration();
		ec.setUseDisk(true);
		teiidServer = new EmbeddedServer();
		teiidServer.start(ec);
		teiidServer.addConnectionFactoryProvider("cassandraclustersourcejndi1", 
				new EmbeddedServer.SimpleConnectionFactoryProvider<DataSource>(h2ds1));
		teiidServer.addConnectionFactoryProvider("cassandraclustersourcejndi2", 
				new EmbeddedServer.SimpleConnectionFactoryProvider<DataSource>(h2ds2));
		
		setUpViews();
		teiidServer.addTranslator(TimeSeriesTranslator.class);
		teiidServer.deployVDB("TestVdb1", sourceModel1, viewModel1);
		teiidServer.deployVDB("TestVdb2", sourceModel2, viewModel2);

		teiidServer.deployVDB("TestGlobalVdb", sourceModel1, viewModel1, sourceModel2, viewModel2, globalViewModel);
		//teiidServer.deployVDB("CassandraTranslatorTest", sourceModel1, viewModel);
		td = teiidServer.getDriver();
		viewCon1 = td.connect("jdbc:teiid:TestVdb1", null);
		//DatabaseMetaData dmd = viewCon1.getMetaData();
		
		//ResultSet rs = dmd.getTables(null, null, null, null);
		//while (rs.next()) {
		//	System.out.println(rs.getString("TABLE_TYPE") + "--" + rs.getString("TABLE_CAT") + "--" + rs.getString("TABLE_SCHEM") + "--" + rs.getString("TABLE_NAME"));
		//}
		viewCon2 = td.connect("jdbc:teiid:TestVdb2", null);
		globalViewCon = td.connect("jdbc:teiid:TestGlobalVdb", null);
		//dmd = globalViewCon.getMetaData();
		//rs = dmd.getTables(null, null, null, null);
		//while (rs.next()) {
		//	System.out.println(rs.getString("TABLE_TYPE") + "--" + rs.getString("TABLE_CAT") + "--" + rs.getString("TABLE_SCHEM") + "--" + rs.getString("TABLE_NAME"));
		//}
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
		//cleanUpStorage();
		//deleteNodeData();
		
		viewCon1.close();
		srcCon1.close();
		srcCon2.close();
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
		config2 = new TimeSeriesConfig("TestCluster", "ZIPF_Sample1", 0, Frequency.MILLIS, DynamicTypeTranslator.getViewDataType(View.INTEGERS), "skchEX_1000_1_10;ptrn_24_1_100_10_1", 
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
		HandlerFactory.getMasterDataHandler().createMasterData(config1);
		HandlerFactory.getMasterDataHandler().createMasterData(config2);
		for (TimeSeriesConfig tsc: freqConfigs.values())
			HandlerFactory.getMasterDataHandler().createMasterData(tsc);
	}
	
	protected static Map<Integer, List<Object>> execute(Connection connection, String sql, boolean closeConn) 
			throws Exception {
		Map<Integer, List<Object>> output = null;
		try {
			//connection.setAutoCommit(false);
			connection.setAutoCommit(true);
			Statement statement = connection.createStatement();
			//statement.setFetchSize(1);
			boolean hasResults = statement.execute(sql);
			if (hasResults) {
				output = new HashMap<Integer, List<Object>>();
				ResultSet results = statement.getResultSet();
				
				ResultSetMetaData metadata = results.getMetaData();
				int columns = metadata.getColumnCount();
				System.out.println("Results");
				
				for (int row = 1; results.next(); row++) {
					System.out.print(row + ": ");
					Integer rowInteger = new Integer(row);
					output.put(rowInteger, new ArrayList<Object>());
					for (int i = 0; i < columns; i++) {
						Object val;
						//String typeName = metadata.getColumnTypeName(i+1);
						val = results.getObject(i+1);
						if (i > 0) {
							System.out.print("--");
						}
						output.get(rowInteger).add(val);
						System.out.print(val);
					}
					System.out.println();
				}
				results.close();
			}
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null && closeConn) {
				connection.close();
			}
		}
		return output;
	}
	
	protected static void indexSampleData() throws IOException, TimeSeriesException {
		
		int count = 1 + 1 + freqConfigs.size();
		for (int i = 0; i < count ; i++) {
			SyncWorker sw = new SyncWorker();
			sw.initialize();
			sw.register();
		}
		
		IndexingWorker w1 = new IndexingWorker(IndexType.ptrn);
		w1.initialize();
		w1.register();
		
		IndexingWorker w2 = new IndexingWorker(IndexType.skchEX);
		w2.initialize();
		w2.register();
	
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
		boolean inserted = HandlerFactory.getDataHandler().insertBulkData(serializedBytes);
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
		boolean inserted = HandlerFactory.getDataHandler().insertBulkData(serializedBytes);
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
