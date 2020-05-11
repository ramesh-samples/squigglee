// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.serializers.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
//import org.teiid.translator.TranslatorException;









import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.serializers.ITimeSeriesDeserializer;

public class AvroTimeSeriesDeserializer implements ITimeSeriesDeserializer {

	AvroTimeSeriesHandler handler = null;
	//private byte[] rawData = null;
	GenericRecord timeSeriesRecord = null;
	GenericArray<GenericRecord> blocks;
	
	public AvroTimeSeriesDeserializer() throws TimeSeriesException {
		handler = new AvroTimeSeriesHandler();
	}
	
	@Override
	public void setRawData(byte[] rawData) throws TimeSeriesException {
		//this.rawData = rawData;
		timeSeriesRecord = handler.deserialize(rawData);
		blocks = handler.getBlocks(timeSeriesRecord);
	}

	@Override
	public int getBlockCount() {
		return blocks.size();
	}

	@Override
	public int getDataCount(int blockNumber) {
		 return handler.getData(blocks.get(blockNumber)).size();
	}

	@Override
	public long getOffset(int blockNumber, int dataNumber) {
		return handler.getOffset(handler.getData(blocks.get(blockNumber)).get(dataNumber));
	}

	@Override
	public Object getVal(int blockNumber, int dataNumber) {
		return handler.getDataVal(handler.getData(blocks.get(blockNumber)).get(dataNumber));
	}

	@Override
	public long getStartts(int blockNumber) {
		return handler.getStartts(blocks.get(blockNumber));
	}

	@Override
	public String getCluster(int blockNumber) {
		return handler.getCluster(blocks.get(blockNumber));
	}
	
	@Override
	public String getGuid(int blockNumber) {
		return handler.getGuid(blocks.get(blockNumber));
	}
	
	@Override
	public int getLn(int blockNumber) {
		return handler.getLn(blocks.get(blockNumber));
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
