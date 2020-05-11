package com.squigglee.coord.storage;

import java.util.List;
import java.util.Map;

import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.Query;
import com.squigglee.core.entity.Stream;
import com.squigglee.core.entity.SyncTask;
import com.squigglee.core.entity.TimeSeriesException;

public class CEPDataHandlerMixin {
	
	public void addStream(String cluster, int ln, Stream stream) throws TimeSeriesException {
		ServiceFactory.getCEPService().addStream(cluster, ln, stream);
	}
	
	public void addQuery(String cluster, int ln, Query query) throws TimeSeriesException {
		ServiceFactory.getCEPService().addQuery(cluster, ln, query);
	}
	
	public void removeStream(String cluster, int ln, String streamGuid) throws TimeSeriesException {
		ServiceFactory.getCEPService().removeStream(cluster, ln, streamGuid);
	}
	
	public void removeQuery(String cluster, int ln, String queryGuid) throws TimeSeriesException {
		ServiceFactory.getCEPService().removeQuery(cluster, ln, queryGuid);
	}

	public Map<String, Map<Integer, List<Stream>>> getStreams(String cluster, int ln) throws TimeSeriesException {
		return ServiceFactory.getCEPService().getStreams(cluster, ln);
	}

	public List<Query> getQueries(String cluster, int ln) throws TimeSeriesException {
		return ServiceFactory.getCEPService().getQueries(cluster, ln);
	}
	
	public void updataCEPSyncStatus(SyncTask syncTask) throws TimeSeriesException {
		ServiceFactory.getCEPService().logCEPSync(syncTask, LocalNodeProperties.getClusterName(), LocalNodeProperties.getNodeLogicalNumber());	
	}
	
	public List<com.squigglee.core.entity.Event> getEvents(String cluster, int ln, String streamId, int count, boolean last) throws TimeSeriesException {
		return ServiceFactory.getCEPService().getEvents(cluster, ln, streamId, count, last);
	}
}
