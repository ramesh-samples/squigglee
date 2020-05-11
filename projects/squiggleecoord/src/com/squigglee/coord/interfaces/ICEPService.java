package com.squigglee.coord.interfaces;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.Query;
import com.squigglee.core.entity.Stream;
import com.squigglee.core.entity.SyncTask;

public interface ICEPService extends ISyncService {
	public List<Query> getQueries(String cluster, int ln);
	public Map<String,Map<Integer,List<Stream>>> getStreams(String cluster, int ln);
	public void addStream(String cluster, int ln, Stream stream);
	public void addQuery(String cluster, int ln, Query query);
	public void removeStream(String cluster, int ln, String streamGuid);
	public void removeQuery(String cluster, int ln, String queryGuid);
	public void logCEPSync(SyncTask task, String cluster, int ln);
	public void initializeStream(String cluster, int ln, String streamId);
	public void writeEvent(String cluster, int ln, String streamId, long startts, int offset, Object value, long eventTime);
	public List<com.squigglee.core.entity.Event> getEvents(String cluster, int ln, String streamId, int count, boolean last);
	public Map<String,List<Integer>> getInterestedLocations(SyncTask task);
	public void sweepAndPurge(String cluster, int ln, int TTL);
}
