// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.serializers;

import org.apache.avro.Schema.Type;
//import org.teiid.translator.TranslatorException;





import com.squigglee.core.entity.TimeSeriesException;

public interface ITimeSeriesDeserializer {

	public void setRawData(byte[] rawData) throws TimeSeriesException;
	public int getBlockCount();
	public int getDataCount(int blockNumber);
	public long getOffset(int blockNumber, int dataNumber);
	public Object getVal(int blockNumber, int dataNumber);
	public long getStartts(int blockNumber);
	public String getGuid(int blockNumber);
	public String getCluster(int blockNumber);
	public int getLn(int blockNumber);
	void resetSchema(Type dataType) throws TimeSeriesException;
	public Type getDataType() throws TimeSeriesException;
	
}
