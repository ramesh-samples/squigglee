// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.interfaces;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.TimeSeriesException;

public interface IIndexHandler extends IMasterDataHandler {
	public byte[] loadSerializedIndex(String cluster, long id, String idxTableName) throws TimeSeriesException;
	public void saveSerializedIndex(String cluster, long id, String idxTableName, byte[] srlIndex, boolean create) throws TimeSeriesException;
	public Map<Long,Map<IndexType,List<String>>> getIndexList(String cluster, int ln) throws TimeSeriesException;
	public List<String> parseIndexString(IndexType indexType, String indexString);
	public boolean getConfigurationStatus(String cluster, long id, String index);
}
