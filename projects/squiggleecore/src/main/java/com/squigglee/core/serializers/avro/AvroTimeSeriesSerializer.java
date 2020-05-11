// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.serializers.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
//import org.teiid.translator.TranslatorException;









import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.serializers.ITimeSeriesSerializer;

public class AvroTimeSeriesSerializer implements ITimeSeriesSerializer {
	private static Logger logger = Logger.getLogger("com.squigglee.core.serializers.avro.AvroTimeSeriesSerializer");
	AvroTimeSeriesHandler handler = null;
	GenericArray<GenericRecord> blockArray = null;
	long currentBlockStartts = 0;
	String currentBlockGuid = null;
	int currentBlockLn = -1;
	String currentBlockCluster = null;
	GenericArray<GenericRecord> currentDataArray = null;
	
	public AvroTimeSeriesSerializer() throws TimeSeriesException {
		handler = new AvroTimeSeriesHandler();
	}
	
	@Override
	public void setBlockCount(int blockCount) {
		blockArray = new GenericData.Array<>(blockCount, handler.getSchema().getBlockArraySchema());
	}
	
	@Override
	public byte[] getRawData() throws TimeSeriesException {
		byte[] data = null;
		//TODO do you need this line below?
		startNewBlock(null, -1,null,0,0);
		try {
			data = handler.serialize(handler.setTimeSeriesRecord(blockArray));
		} catch (IOException ioe) {
			logger.error("Error serializing time series data",ioe);
			throw new TimeSeriesException(ioe);
		}
		return data;
	}

	@Override
	public void startNewBlock(String cluster, int ln, String guid, long startts, int dataSize) throws TimeSeriesException {
		if (blockArray == null)
			throw new TimeSeriesException("Must set the number of blocks before starting a new block");
		if (currentDataArray != null && !currentDataArray.isEmpty())
			blockArray.add(handler.setBlockRecord(currentBlockCluster, currentBlockLn, currentBlockGuid, currentBlockStartts, currentDataArray));
		if (ln < 0 || guid == null || dataSize == 0)
			return;
		currentBlockGuid = guid;
		currentBlockLn = ln;
		currentBlockCluster = cluster;
		currentBlockStartts = startts;
		currentDataArray = new GenericData.Array<>(dataSize, handler.getSchema().getDataArraySchema());
//		
//		if (!handler.getSchema().getCurrentDataType().equals(Schema.Type.DOUBLE))
//			handler.getSchema().resetSchema(Schema.Type.DOUBLE);
	}

	@Override
	public void setData(long offset, Object val) throws TimeSeriesException {
		if (currentDataArray == null)
			throw new TimeSeriesException("Must call startNewBlock method on serializer before setting data");
		//Schema.Type dataType = DynamicTypeTranslator.getRuntimeType(val);
		//handler.getSchema().currentDataType = Schema.Type.DOUBLE;
		currentDataArray.add(handler.setDataRecord(offset, val));
	}

	@Override
	public void reset() {
		currentDataArray = null;
		blockArray = null;
		currentBlockGuid = null;
		currentBlockStartts = 0;
	}
	
	@Override
	public void resetSchema(Schema.Type dataType) throws TimeSeriesException {
		if (!this.handler.getSchema().getCurrentDataType().equals(dataType))
			this.handler.getSchema().resetSchema(dataType);
	}
	
	@Override
	public Schema.Type getDataType() throws TimeSeriesException {
		return handler.getSchema().getCurrentDataType();
	}	
}
