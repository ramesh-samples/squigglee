// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.interfaces;

import java.util.SortedMap;

import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.SyncTask;
import com.squigglee.core.entity.TimeSeriesException;

public interface IDataHandler extends IMasterDataHandler {
	public boolean insertBulkData(byte[] bulkData) throws TimeSeriesException;
	public boolean updateBulkData(byte[] bulkData) throws TimeSeriesException;
	public boolean deleteData(String cluster, int ln, String guid, long begints, long beginoffset, long endts, long endoffset) throws TimeSeriesException;
	public boolean deleteData(MasterData md, long start, long end) throws TimeSeriesException;
	
	public Object[] readBlockData(String cluster, int ln, String guid, long begints, long beginoffset, long endts, long endoffset) throws TimeSeriesException;
	public Object[] readBlockData(MasterData md, long start, long end) throws TimeSeriesException;
	public SortedMap<Long, Object> fetchTimeSeries(byte[] data) throws TimeSeriesException;
	public byte[] getSerializedData(MasterData md, SortedMap<Long, Object> data) throws TimeSeriesException;
	public void deleteNodeData(String cluster, int ln) throws TimeSeriesException;
	SortedMap<Long, Object> fetchTimeSeriesLimit(MasterData md, long start, long end, int limit, boolean last) throws TimeSeriesException;
	public SortedMap<Long, Object> fetchTimeSeries(MasterData md, long startOffset, long endOffset) throws TimeSeriesException;
	
	public boolean syncData(SyncTask syncTask) throws TimeSeriesException;
	public void updataSyncStatus(SyncTask syncTask) throws TimeSeriesException;
	public void postIndexingJobs(SyncTask task) throws TimeSeriesException;
}
