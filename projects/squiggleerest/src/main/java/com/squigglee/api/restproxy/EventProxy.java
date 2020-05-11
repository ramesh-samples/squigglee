package com.squigglee.api.restproxy;


import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.core.entity.Event;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.ICEPDataHandler;
import com.squigglee.core.interfaces.IStatusHandler;

public class EventProxy {
	private static Logger logger = Logger.getLogger("com.squigglee.api.restproxy.TimeSeriesProxy");
	protected int localLn = 0;
	protected int limit = 0;
	protected String localCluster = null;
	protected ICEPDataHandler cepHandler = null;
	protected IStatusHandler statusHandler = null;
	
	public EventProxy(ICEPDataHandler cepHandler, IStatusHandler statusHandler, int localLn, String localCluster, int limit) {
		this.cepHandler = cepHandler;
		this.statusHandler = statusHandler;
		this.localLn = localLn;
		this.localCluster = localCluster;
		this.limit = limit;
	}

	public List<Event> getEvents(String cluster, int ln, String streamId, int count, boolean last) {
		List<Event> events = new ArrayList<Event>();
		try {
			if (ln == localLn) {
				events = cepHandler.getEvents(cluster, ln, streamId, count, last);
				System.out.println("Making local request for stream id = " +streamId + 
						(last?" last ":" first ") + count + " events " + " at node = " + ln + " cluster = " + cluster);
				logger.debug("Making local request for stream id = " +streamId + 
						(last?" last ":" first ") + count + " events " + " at node = " + ln + " cluster = " + cluster);
			}
			else {
				NodeStatus ns = statusHandler.fetchNodeStatus(cluster, ln);
				System.out.println("Making proxy request for stream id = " +streamId + 
						(last?" last ":" first ") + count + " events " + " at node = " + ln + " cluster = " + cluster);
				logger.debug("Making proxy request for stream id = " +streamId + 
						(last?" last ":" first ") + count + " events " + " at node = " + ln + " cluster = " + cluster);
				events = RESTFactory.getEventProxy(ns.getAddress()).getEvents(cluster, ln, streamId, count, last);
			}
		} catch (TimeSeriesException e) {
			logger.error("Making error fetching events for stream id = " +streamId + 
					(last?" last ":" first ") + count + " events " + " at node = " + ln + " cluster = " + cluster);
		}
		return events;
	}
}
