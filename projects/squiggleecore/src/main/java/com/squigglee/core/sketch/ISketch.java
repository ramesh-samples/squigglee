// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.sketch;

import java.io.Externalizable;

import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IIndexHandler;

public interface ISketch extends Externalizable {
	public void update(long index, double val);
	public void reverseUpdate(long index, double val);
	public long pointQuery(double val);
	public long rangeQuery(double start, double end);
	public Stats statistics();
	public void updateIndex(String cluster, long id, IIndexHandler handler, boolean create) throws TimeSeriesException;
	public void loadSerializedIndex(String cluster, long id, String sketchIndexTbl, IIndexHandler handler);
	public byte[] serializeIndex();
	public void deserializeIndex(byte[] idx);
	public String getTableName();
	public void setValuesFromTableName(String skchTbl);
}
