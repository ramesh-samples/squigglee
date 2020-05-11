// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.h2db;

import java.util.List;

import org.apache.log4j.Logger;

import com.squigglee.coord.interfaces.IData;
import com.squigglee.coord.interfaces.IStatus;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.interfaces.IStatusHandler;
import com.squigglee.core.interfaces.NodeStatus;
import com.squigglee.core.interfaces.StatusType;
import com.squigglee.core.interfaces.TimeSeriesException;

public class StatusHandlerImpl extends SchemaHandlerImpl implements IStatusHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.StatusHandlerImpl");

	protected static int TTL = 60; // Status time to live in seconds 

	//call on create request
	@Override
	public void updateNode(int ln, String addr, String dataCenter, String instanceId, String name, boolean isBoot, boolean isSeed, 
			int replicaOf, int storage, String stype) throws TimeSeriesException {
		IData dataService = ServiceFactory.getDataService();
		dataService.setNode(this.clusterName, ln, addr, dataCenter, instanceId, isBoot, isSeed, name, replicaOf, storage, stype);
		logger.debug("Updated node status for node = " + ln);
		//dataService.close();
	}
	
	@Override
	public void updateIndexServiceStatus(int ln) throws TimeSeriesException {
		updateStatus(ln, StatusType.COORDINATION);
	}

	@Override
	public void updateOverlayServiceStatus(int ln) throws TimeSeriesException {
		updateStatus(ln,StatusType.OVERLAY);
	}

	@Override
	public void updateViewStatus(int ln) throws TimeSeriesException {
		updateStatus(ln,StatusType.VIEW);
	}

	@Override
	public void updateGlobalViewStatus(int ln) throws TimeSeriesException {
		updateStatus(ln,StatusType.VIEW);
	}
	
	private void updateStatus(int ln, StatusType statusType) throws TimeSeriesException {
		IStatus statusService = ServiceFactory.getStatusService();
		statusService.setClusterStatus(this.clusterName, ln, statusType);		
		logger.debug("Updated node status for node = " + ln + " and status type = " + statusType);
		//statusService.close();
	}

	@Override
	public List<NodeStatus> fetchClusterStatus() throws TimeSeriesException {
		IStatus statusService = ServiceFactory.getStatusService();
		List<NodeStatus> clusterStatus = statusService.getClusterStatus(this.clusterName);
		//statusService.close();
		return clusterStatus;
	}
	
	@Override
	public void initialize() {
		super.initialize();
	}

	@Override
	public void deleteNode(int ln) throws TimeSeriesException {
		IData dataService = ServiceFactory.getDataService();
		dataService.deleteNode(this.clusterName, ln);
		logger.debug("Deleted data for node = " + ln);
		//dataService.close(); 
	}

}
