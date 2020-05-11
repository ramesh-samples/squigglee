// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.serializers;

import org.apache.avro.Schema.Type;
//import org.teiid.translator.TranslatorException;





import com.squigglee.core.entity.TimeSeriesException;

public interface ITimeSeriesSerializer {

	public byte[] getRawData() throws TimeSeriesException;
	public void setBlockCount(int blockCount);
	public void reset();
	void setData(long offset, Object val) throws TimeSeriesException;
	void startNewBlock(String cluster, int ln, String guid, long startts, int dataSize) throws TimeSeriesException;
	void resetSchema(Type dataType) throws TimeSeriesException;
	Type getDataType() throws TimeSeriesException;
}
