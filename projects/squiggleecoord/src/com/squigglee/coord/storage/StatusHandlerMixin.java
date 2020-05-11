// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.storage;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.interfaces.IStatusService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.StatusType;
import com.squigglee.core.entity.TimeSeriesException;

public class StatusHandlerMixin  {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.StatusHandlerImpl");
	protected static int TTL = 60; // Status time to live in seconds 

	public void updateNodeStatus(String cluster, int ln) throws TimeSeriesException {
		updateStatus(cluster, ln, StatusType.NODE);
	}

	public void updateOverlayStatus(String cluster, int ln) throws TimeSeriesException {
		updateStatus(cluster, ln, StatusType.OVERLAY);
	}
	
	private void updateStatus(String cluster, int ln, StatusType statusType) throws TimeSeriesException {
		IStatusService statusService = ServiceFactory.getStatusService();
		statusService.setClusterStatus(cluster, ln, statusType);		
		logger.debug("Updated node status for node = " + ln + " and status type = " + statusType);
	}

	public List<NodeStatus> fetchClusterStatus(String cluster) throws TimeSeriesException {
		IStatusService statusService = ServiceFactory.getStatusService();
		List<NodeStatus> clusterStatus = statusService.getClusterStatus(cluster);
		return clusterStatus;
	}
	
	public NodeStatus fetchNodeStatus(String cluster, int ln) throws TimeSeriesException {
		IStatusService statusService = ServiceFactory.getStatusService();
		return statusService.getNodeStatus(cluster, ln);
	}
	
	public Map<String,List<NodeStatus>> fetchGlobalStatus() throws TimeSeriesException {
		return ServiceFactory.getStatusService().getGlobalStatus();
	}

	public void deleteNode(String cluster, int ln) throws TimeSeriesException {
		IDataService dataService = ServiceFactory.getDataService();
		dataService.deleteNode(cluster, ln);
		logger.debug("Deleted data for node = " + ln);
	}

}
