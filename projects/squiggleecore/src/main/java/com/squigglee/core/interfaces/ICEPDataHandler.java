package com.squigglee.core.interfaces;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.Query;
import com.squigglee.core.entity.Stream;
import com.squigglee.core.entity.SyncTask;
import com.squigglee.core.entity.TimeSeriesException;

public interface ICEPDataHandler extends IDataHandler {
	public void addStream(String cluster, int ln, Stream stream) throws TimeSeriesException;
	public void addQuery(String cluster, int ln, Query query) throws TimeSeriesException;
	public void removeStream(String cluster, int ln, String streamGuid) throws TimeSeriesException;
	public void removeQuery(String cluster, int ln, String queryGuid) throws TimeSeriesException;
	public Map<String,Map<Integer,List<Stream>>> getStreams(String cluster, int ln) throws TimeSeriesException;
	public List<Query> getQueries(String cluster, int ln) throws TimeSeriesException;
	public void updataCEPSyncStatus(SyncTask syncTask) throws TimeSeriesException;
	public List<com.squigglee.core.entity.Event> getEvents(String cluster, int ln, String streamId, int count, boolean last) throws TimeSeriesException;
}
