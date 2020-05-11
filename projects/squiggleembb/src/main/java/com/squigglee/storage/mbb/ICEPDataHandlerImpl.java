// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mbb;

import java.util.List;
import java.util.Map;

import com.squigglee.coord.storage.CEPDataHandlerMixin;
import com.squigglee.core.entity.Query;
import com.squigglee.core.entity.Stream;
import com.squigglee.core.entity.SyncTask;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.ICEPDataHandler;

public class ICEPDataHandlerImpl extends IDataHandlerImpl implements ICEPDataHandler {
	//private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.ICEPDataHandlerImpl");
	protected static CEPDataHandlerMixin cepMixin = null;
	
	@Override
	public void initialize() {
		super.initialize();
		if (cepMixin == null)
			cepMixin = new CEPDataHandlerMixin();
	}

	@Override
	public void addStream(String cluster, int ln, Stream stream) throws TimeSeriesException {
		cepMixin.addStream(cluster, ln, stream);
	}

	@Override
	public void addQuery(String cluster, int ln, Query query) throws TimeSeriesException {
		cepMixin.addQuery(cluster, ln, query);
	}

	@Override
	public void removeStream(String cluster, int ln, String streamGuid) throws TimeSeriesException {
		cepMixin.removeStream(cluster, ln, streamGuid);
	}

	@Override
	public void removeQuery(String cluster, int ln, String queryGuid) throws TimeSeriesException {
		cepMixin.removeQuery(cluster, ln, queryGuid);
	}

	@Override
	public Map<String, Map<Integer, List<Stream>>> getStreams(String cluster, int ln) throws TimeSeriesException {
		return cepMixin.getStreams(cluster, ln);
	}

	@Override
	public List<Query> getQueries(String cluster, int ln) throws TimeSeriesException {
		return cepMixin.getQueries(cluster, ln);
	}
	
	@Override
	public void updataCEPSyncStatus(SyncTask syncTask) throws TimeSeriesException {
		cepMixin.updataCEPSyncStatus(syncTask);
	}
	
	public List<com.squigglee.core.entity.Event> getEvents(String cluster, int ln, String streamId, int count, boolean last) throws TimeSeriesException {
		return cepMixin.getEvents(cluster, ln, streamId, count, last);
	}
}
