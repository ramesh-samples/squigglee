// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mapdb;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.squigglee.coord.interfaces.IServiceFactory;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.IHandler;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.serializers.avro.AvroTimeSeriesDeserializer;
import com.squigglee.core.serializers.avro.AvroTimeSeriesSerializer;
import com.squigglee.core.serializers.avro.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.ITimeSeriesDeserializer;
import com.squigglee.core.serializers.avro.ITimeSeriesSerializer;

public class HandlerBase implements IHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mapdb.HandlerBase");
	protected String clusterName = null;
	protected int clusterPort;
	protected String clusterSeeds = null;
	protected String localDataCenter = null;
	protected ITimeSeriesDeserializer deserializer = null;
	protected ITimeSeriesSerializer serializer = null;
	protected String address = null;
	protected IServiceFactory serviceFactory = null;
	protected static String storagePath = "/Users/AgnitioWorks/Documents/tsr/mapdb/id_";
	protected SortedMap<String, DB> _ENGINES = null;
	protected SortedMap<String, Map<String, ConcurrentNavigableMap<Object,Object>>> _TABLES = null;
	
	public HandlerBase() {
		try {
			this.clusterName = LocalNodeProperties.getClusterName();
			this.clusterPort = LocalNodeProperties.getClusterPort();
			this.clusterSeeds = LocalNodeProperties.getClusterSeeds();
			this.localDataCenter = LocalNodeProperties.getLocalDataCenter();
			this.address = LocalNodeProperties.getNodeAddress();
			if (LocalNodeProperties.getSerializerType().equals(TsrConstants.HANDLER_SERIALIZER_AVRO)) {
				this.deserializer = new AvroTimeSeriesDeserializer();
				this.serializer = new AvroTimeSeriesSerializer();
			}
		} catch (TimeSeriesException tse) {
			logger.error("Failed to initialize handler", tse);
		}
		initialize();
	}

	static {
		
	}
	
	public ConcurrentNavigableMap<Object,Object> getTable(String id, String dataType) {
		
		if (!_ENGINES.containsKey(id)) {
			_ENGINES.put("" + id, DBMaker.newFileDB(new File(storagePath + id))
					.closeOnJvmShutdown()
					//.transactionDisable()
					.mmapFileEnablePartial()
					.cacheLRUEnable()
					.cacheSize(1000000)
					.make());
			//_ENGINES.put("" + id, DBMaker.newFileDB(new File(storagePath + id)).transactionDisable().closeOnJvmShutdown().make());
		}
		
		if (!_TABLES.containsKey(id))
			_TABLES.put("" + id, new HashMap<String, ConcurrentNavigableMap<Object,Object>>());
		
		if (!_TABLES.get(id).containsKey(dataType)) {
			_TABLES.get(id).put(dataType, _ENGINES.get(id).createTreeMap(dataType).makeOrGet());
		}
		
		return _TABLES.get(id).get(dataType);
	}
	
	
	public DB getDatabase(String id) {
		DB db = DBMaker.newFileDB(new File(storagePath + id))
				.closeOnJvmShutdown()
				//.transactionDisable()
				.mmapFileEnablePartial()
				.cacheLRUEnable()
				.cacheSize(1000000)
				.make();
		
		return db;
			
		//return _ENGINES.get(id);
	}
	
	
	public void commit(String id) {
		if (_ENGINES.containsKey(id))
			_ENGINES.get(id).commit();
	}
	
	@Override
	public void initialize() {
		serviceFactory = new ServiceFactory();
		//_ENGINES = new TreeMap<String,DB>();
		//_TABLES = new TreeMap<String, Map<String, ConcurrentNavigableMap<Object,Object>>>();
		try {
			storagePath = LocalNodeProperties.getStoragePath();
			_ENGINES = new TreeMap<String, DB>();
			_TABLES = new TreeMap<String, Map<String, ConcurrentNavigableMap<Object,Object>>>();
		} catch (TimeSeriesException e) {
			logger.error("Fatal initialization error, could not set the storage path", e);
		}
	}

	@Override
	public void reset(String dataType) throws TimeSeriesException {
		Schema.Type schemaType = DynamicTypeTranslator.getSchemaType(dataType);
		this.serializer.resetSchema(schemaType);
		this.deserializer.resetSchema(schemaType);
	}
	
	@Override
	public void shutdown() {
		//for (DB db : _ENGINES.values())
		//	if (!db.isClosed())
		//		db.close();
	}

}