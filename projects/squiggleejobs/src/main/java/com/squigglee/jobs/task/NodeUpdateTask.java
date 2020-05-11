// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.task;

import org.apache.log4j.Logger;

import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;

public class NodeUpdateTask {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.task.NodeUpdateTask");
	private IDataService dataService = null;
	private int ln = -1;
	public NodeUpdateTask () throws TimeSeriesException {
		initialize();
	}
	
	public void updateNodeProperties() {
		try { 
			System.out.println("Updating node properties for logical node = " + ln);
			logger.debug("Updating node properties for logical node = " + ln);
			dataService.setupCluster(LocalNodeProperties.getClusterName());
			dataService.setNode(LocalNodeProperties.getClusterName(), LocalNodeProperties.getNodeLogicalNumber(), LocalNodeProperties.getNodeAddress(), 
					LocalNodeProperties.getNodeDataCenter(), LocalNodeProperties.getInstanceId(), LocalNodeProperties.isBoostrapNode(), 
					LocalNodeProperties.isSeedNode(), LocalNodeProperties.getNodeName(), LocalNodeProperties.isReplicaOf(), LocalNodeProperties.getServerStorage(), 
					LocalNodeProperties.getServerType());
		} catch (TimeSeriesException tse) {
			logger.error("Failed to run node update task task for logical node = " + ln,tse);
		}
	}
	
	private void initialize() throws TimeSeriesException {
		ln = LocalNodeProperties.getNodeLogicalNumber();
		dataService = ServiceFactory.getDataService();
	}
	
    public static void main (String[] args) throws TimeSeriesException {
    	(new NodeUpdateTask()).updateNodeProperties();
    	System.exit(0);
    }
}
