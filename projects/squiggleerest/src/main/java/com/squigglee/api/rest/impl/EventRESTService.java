package com.squigglee.api.rest.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import com.squigglee.api.rest.IEventRESTService;
import com.squigglee.core.entity.Event;
import com.squigglee.core.entity.Query;
import com.squigglee.core.entity.Stream;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;

public class EventRESTService extends RestBase implements IEventRESTService {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.StatusRESTService");

	@Override
	public Map<String, Map<Integer, List<Stream>>> getStreams(String cluster,
			int ln) {
		try {
			return HandlerFactory.getCEPDataHandler().getStreams(cluster, ln);
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching streams at node " + ln + " in cluster " + cluster, e);
		}
		return new HashMap<String, Map<Integer, List<Stream>>>();
	}

	@Override
	public void addStream(String cluster, int ln, Stream stream) {
		try {
			HandlerFactory.getCEPDataHandler().addStream(cluster, ln, stream);
		} catch (TimeSeriesException e) {
			logger.error("Found error adding stream " + stream + " at node " + ln + " in cluster " + cluster, e);
		}
	}

	@Override
	public void removeStream(String cluster, int ln, String streamId) {
		try {
			HandlerFactory.getCEPDataHandler().removeStream(cluster, ln, streamId);
		} catch (TimeSeriesException e) {
			logger.error("Found error removing stream id " + streamId + " at node " + ln + " in cluster " + cluster, e);
		}
	}

	@Override
	public List<Query> getQueries(String cluster, int ln) {
		try {
			return HandlerFactory.getCEPDataHandler().getQueries(cluster, ln);
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching queries at node " + ln + " in cluster " + cluster, e);
		}
		return new ArrayList<Query>();
	}

	@Override
	public void addQuery(String cluster, int ln, Query query) {
		try {
			HandlerFactory.getCEPDataHandler().addQuery(cluster, ln, query);
		} catch (TimeSeriesException e) {
			logger.error("Found error adding query " + query + " at node " + ln + " in cluster " + cluster, e);
		}
	}

	@Override
	public void removeQuery(String cluster, int ln, String queryId) {
		try {
			HandlerFactory.getCEPDataHandler().removeQuery(cluster, ln, queryId);
		} catch (TimeSeriesException e) {
			logger.error("Found error removing query id " + queryId + " at node " + ln + " in cluster " + cluster, e);
		}
	}

	@Override
	public List<Event> getEvents(String cluster, int ln, String streamId, int count, boolean last) {
		return eventProxy.getEvents(cluster, ln, streamId, count, last);
	}
}
