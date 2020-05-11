// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.task;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.squigglee.coord.interfaces.IStatusService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.StatusType;
import com.squigglee.core.entity.TimeSeriesException;

public class PingTask {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.task.PingTask");
	protected static IStatusService statusService = null;
	private static int localLn = 0;
	private static String localCluster = null;
	private int interval = 10;
    private static final ScheduledExecutorService scheduler =
		     Executors.newScheduledThreadPool(1);
	
	public PingTask () 
			throws TimeSeriesException {
		initialize();
	}

	static {
		try {
			statusService = ServiceFactory.getStatusService();
			localLn = LocalNodeProperties.getNodeLogicalNumber();
			localCluster = LocalNodeProperties.getClusterName();
		} catch (TimeSeriesException e) {
			logger.error("Failed to initialize PingTask", e);
		}
	}
	
	private void initialize() throws TimeSeriesException {
		logger.debug("Initializing and scheduling first ping job for logical node = " + localLn + " and cluster " + localCluster);
		System.out.println("Initializing and scheduling first ping job for logical node = " + localLn + " and cluster " + localCluster);
		scheduler.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				logger.debug("Pinging coordination services at node " + localLn + " in cluster " + localCluster + " at time " + DateTime.now());
				System.out.println("Pinging coordination services at node " + localLn + " in cluster " + localCluster + " at time " + DateTime.now());
				try {
					statusService.executeLine("ls /SQUIGGLEE/" + localCluster);
					statusService.setClusterStatus(localCluster, localLn, StatusType.NODE);
				} catch (TimeSeriesException tse) {
					logger.error("Failed to run ping task for logical node = " + localLn,tse);
				}				
				try {
					statusService.executeLineOv("ls /SQUIGGLEE/" + localCluster);
					statusService.setClusterStatus(localCluster, localLn, StatusType.OVERLAY);
				} catch (TimeSeriesException tse) {
					logger.error("Failed to run ping task for logical node = " + localLn,tse);
				}
			}}, interval, interval, TimeUnit.SECONDS);
	}
}
