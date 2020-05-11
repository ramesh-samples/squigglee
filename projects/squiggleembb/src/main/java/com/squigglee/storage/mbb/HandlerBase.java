// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mbb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IHandler;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.ITimeSeriesDeserializer;
import com.squigglee.core.serializers.ITimeSeriesSerializer;
import com.squigglee.core.serializers.avro.AvroTimeSeriesDeserializer;
import com.squigglee.core.serializers.avro.AvroTimeSeriesSerializer;

public class HandlerBase implements IHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.HandlerBase");
	protected String localCluster = null;
	protected int localln = 0;
	protected int replicaOf = 0;
	protected int replicaCount = 0;
	protected int replicaNumber = 0;
	protected String localDataCenter = null;
	protected ITimeSeriesDeserializer deserializer = null;
	protected ITimeSeriesSerializer serializer = null;
	protected String address = null;
	protected String storagePath = "/Users/AgnitioWorks/Documents/tsr/h2db";
	protected int dataThreads = 10;
	protected ExecutorService dataExecutorService;
	//boolean isDataNode = false;
	
	public HandlerBase() {}
	
	protected MappedByteBuffer getMappedBuffer(File f, long position, long size) {
		RandomAccessFile raf = null;
		FileChannel fc = null;
		MappedByteBuffer map = null;
		try {
			raf = new RandomAccessFile(f, "rw");
			fc = raf.getChannel();
			
			int maxTry = 10;
			int retry = 0;
			if (position > fc.size())
				position = fc.size();
			if (size > fc.size())
				size = fc.size();
			
			map = fc.map(MapMode.READ_WRITE, position, size);
			while (map == null) {
				try {
					System.out.println("Retrying to get the index map for position = " + position);
					Thread.sleep(2000);
					map = fc.map(MapMode.READ_WRITE, position, size);
				} catch (Exception e) {
					logger.error("Error sleeping while trying to retrieve the map for file = " + f.getAbsolutePath(),e);
				}
				retry++;
				if (retry == maxTry) {
					logger.error("Failed to retrieve the map for file = " + f.getAbsolutePath());
					break;
				}
			}
		} catch (IOException e) {
			logger.error("Found error getting data buffer for file = " + f==null?"":f.getAbsolutePath(), e);
		} finally {
			try {
				if (fc != null)
					fc.close();
				if (raf != null)
					raf.close();
			} catch (IOException e) {
				logger.error("Found error getting data buffer for file = " + f==null?"":f.getAbsolutePath(), e);
			}
		}
		return map;
	}
	
	protected ByteBuffer getReadOnlyMappedBuffer(File f, long position, long size) {
		return getMappedBuffer(f, position, size).asReadOnlyBuffer();
	}
	
	protected void put(MappedByteBuffer mbb, int index, Object val, String dataType) {
		switch(dataType) {
		case "int":
			if (val != null) {
				mbb.put(index, (byte) 1);
				mbb.putInt(index + 1, Integer.parseInt(val.toString()));
			}
			break;
		case "double":
			if (val != null) {
				mbb.put(index, (byte) 1);
				mbb.putDouble(index + 1, Double.parseDouble(val.toString()));
			}
			break;
		case "long":
			if (val != null) {
				mbb.put(index, (byte) 1);
				mbb.putLong(index + 1, Long.parseLong(val.toString()));
			}
			break;
		}
	}
	
	protected void reset(MappedByteBuffer mbb, int index) {
		mbb.put(index, (byte) 0);
	}
	
	protected Object get(MappedByteBuffer mbb, int index, String dataType) {
		switch(dataType) {
		case "int":
			if ( mbb.get(index) != (byte) 0) {
				return mbb.getInt(index+1);
			}
		case "double":
			if ( mbb.get(index) != (byte) 0) {
				return mbb.getDouble(index+1);
			}
		case "long":
			if ( mbb.get(index) != (byte) 0) {
				return mbb.getLong(index+1);
			}
		}
		return null;
	}
	
	protected boolean isDataNode() {
		return localln == replicaOf;
	}
	
	@Override
	public void initialize() {
		try {
			this.localCluster = LocalNodeProperties.getClusterName();
			this.localDataCenter = LocalNodeProperties.getLocalDataCenter();
			this.address = LocalNodeProperties.getNodeAddress();
			//isDataNode = LocalNodeProperties.getNodeLogicalNumber() == LocalNodeProperties.isReplicaOf();
			storagePath = LocalNodeProperties.getStoragePath();
			this.localln = LocalNodeProperties.getNodeLogicalNumber();
			replicaOf = LocalNodeProperties.isReplicaOf();
			dataThreads = LocalNodeProperties.getDataHandlerThreads();
			dataExecutorService = Executors.newFixedThreadPool(dataThreads);
			if (LocalNodeProperties.getSerializerType().equals(TsrConstants.HANDLER_SERIALIZER_AVRO)) {
				this.deserializer = new AvroTimeSeriesDeserializer();
				this.serializer = new AvroTimeSeriesSerializer();
			}
		} catch (TimeSeriesException tse) {
			logger.error("Failed to initialize handler", tse);
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
	}
}