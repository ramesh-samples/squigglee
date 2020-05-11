// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.h2db;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.h2.jdbcx.JdbcConnectionPool;

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
	private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.HandlerBase");
	protected static String clusterName = null;
	protected static int ln = 0;
	protected String localDataCenter = null;
	protected ITimeSeriesDeserializer deserializer = null;
	protected ITimeSeriesSerializer serializer = null;
	protected String address = null;
	protected static String storagePath = "/Users/AgnitioWorks/Documents/tsr/h2db";
	boolean isDataNode = false;
	protected static JdbcConnectionPool pool = null;
	protected static JdbcConnectionPool batchPool = null;
	
	public HandlerBase() {
		try {
			clusterName = LocalNodeProperties.getClusterName();
			ln = LocalNodeProperties.getNodeLogicalNumber();
			this.localDataCenter = LocalNodeProperties.getLocalDataCenter();
			this.address = LocalNodeProperties.getNodeAddress();
			isDataNode = LocalNodeProperties.getNodeLogicalNumber() == LocalNodeProperties.isReplicaOf();
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
		try {
			Class.forName("org.h2.Driver");
			storagePath = LocalNodeProperties.getStoragePath();
			pool = JdbcConnectionPool.create("jdbc:h2:tcp://localhost/" + storagePath + "/" + ln + ";CACHE_SIZE=131072", "sa","");
			batchPool = JdbcConnectionPool.create("jdbc:h2:tcp://localhost/" + storagePath + "/" + ln + ";LOG=0;LOCK_MODE=0;UNDO_LOG=0;CACHE_SIZE=131072", "sa","");
		} catch (ClassNotFoundException e) {
			logger.error("Failed to H2 driver class", e);
		} catch (TimeSeriesException e) {
			logger.error("Failed to initialize static properties", e);
		}
	}
	
	public Connection getConnection() throws TimeSeriesException {
			try {
				//return DriverManager.getConnection("jdbc:h2:/" + storagePath + "/" + ln + ";AUTO_SERVER=TRUE", "sa","");
				//return DriverManager.getConnection("jdbc:h2:tcp://localhost/" + storagePath + "/" + ln + ";DB_CLOSE_DELAY=10;CACHE_SIZE=131072", "sa","");
				return pool.getConnection();
			} catch (SQLException e) {
				logger.error("Failed to obtain connection", e);
				throw new TimeSeriesException("Failed to obtain connection", e);
			}
	}
	
	public Connection getBatchUpdateConnection() throws TimeSeriesException {
		try {
			//return DriverManager.getConnection("jdbc:h2:/" + storagePath + "/" + ln + ";AUTO_SERVER=TRUE", "sa","");
			//return DriverManager.getConnection("jdbc:h2:tcp://localhost/" + storagePath + "/" + ln 
			//		+ ";LOG=0;CACHE_SIZE=65536;LOCK_MODE=0;UNDO_LOG=0;DB_CLOSE_DELAY=10;CACHE_SIZE=131072", "sa","");
			return batchPool.getConnection();
		} catch (SQLException e) {
			logger.error("Failed to obtain connection", e);
			throw new TimeSeriesException("Failed to obtain connection", e);
		}
	}
	
	@Override
	public void initialize() {
		
		//try {
			
		//} catch (TimeSeriesException e) {
		//	logger.error("Fatal initialization error, could not set the storage path", e);
		//}
	}

	@Override
	public void reset(String dataType) throws TimeSeriesException {
		Schema.Type schemaType = DynamicTypeTranslator.getSchemaType(dataType);
		this.serializer.resetSchema(schemaType);
		this.deserializer.resetSchema(schemaType);
	}
	
	@Override
	public void shutdown() {
	}
}