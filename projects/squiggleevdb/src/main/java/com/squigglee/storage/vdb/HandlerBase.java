// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.vdb;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
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
	private static Logger logger = Logger.getLogger("com.squigglee.storage.vdb.HandlerBase");
	protected String clusterName = null;
	protected int clusterPort;
	protected String clusterSeeds = null;
	protected String localDataCenter = null;
	protected ITimeSeriesDeserializer deserializer = null;
	protected ITimeSeriesSerializer serializer = null;
	protected String address = null;
	protected IServiceFactory serviceFactory = null;
	protected static String storagePath = "/Users/AgnitioWorks/Documents/tsr/mapdb/id_";
	protected static Map<String, MVStore> _ENGINES = null;
	//protected SortedMap<String, DB> _ENGINES = null;
	//protected SortedMap<String, Map<String, ConcurrentNavigableMap<Object,Object>>> _TABLES = null;
	
	public HandlerBase(String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		this.clusterName = clusterName;
		this.clusterPort = clusterPort;
		this.clusterSeeds = clusterSeeds;
		this.localDataCenter = localDataCenter;
		this.address = address;
		if (serializerType.equals(TsrConstants.HANDLER_SERIALIZER_AVRO)) {
			this.deserializer = new AvroTimeSeriesDeserializer();
			this.serializer = new AvroTimeSeriesSerializer();
		}
		initialize();
	}

	static {
		_ENGINES = new HashMap<String, MVStore>();
	}
	/*
	public ConcurrentNavigableMap<Object,Object> getTable(DB db, String dataType) {
		
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
		
		return db.createTreeMap(dataType).makeOrGet();
	}
	*/
	
	public static MVStore getStore(String id) {
		if (!_ENGINES.containsKey(id))
			_ENGINES.put(id,  MVStore.open(storagePath + id));
		return _ENGINES.get(id);
	}
	
	public static MVMap<Object, Object> getMap(MVStore s, String name) {
		return s.openMap(name);
	}
	
	/*
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
	*/
	
//	public void commit(String id) {
//		if (_ENGINES.containsKey(id))
//			_ENGINES.get(id).commit();
//	}
	
	@Override
	public void initialize() {
		serviceFactory = new ServiceFactory();
		//_ENGINES = new TreeMap<String,DB>();
		//_TABLES = new TreeMap<String, Map<String, ConcurrentNavigableMap<Object,Object>>>();
		try {
			storagePath = LocalNodeProperties.getStoragePath();
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