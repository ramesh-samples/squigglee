// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.event;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.joda.time.DateTime;

import com.squigglee.coord.interfaces.ICEPService;
import com.squigglee.coord.interfaces.IWorker;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.coord.utility.ZooKeeperFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.TimeSeriesException;

public class EventSweeper implements IWorker {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.sync.SyncWorker");
	protected static String localCluster = null;
	protected String syncTaskPath = null;
	protected static int localLn = 0;
	protected static int replicaOf = 0;
	protected static String addr = "127.0.0.1";
	protected static int taskCount = 1;
	protected ICEPService cepService = null;
	protected static ZooKeeper zk;
	protected String wguid = null;
	private static int interval = 20;
	private static int TTL = 0;
	private static final ScheduledExecutorService scheduler =
		     Executors.newScheduledThreadPool(1);
	
	static {
		try {
			localCluster = LocalNodeProperties.getClusterName();
			localLn = LocalNodeProperties.getNodeLogicalNumber();
			replicaOf = LocalNodeProperties.isReplicaOf();
			addr = LocalNodeProperties.getNodeAddress();
			taskCount = LocalNodeProperties.getBatchSyncTaskCount();
			zk = ZooKeeperFactory.getLocalZooKeeper();
			TTL = LocalNodeProperties.getCEPEventExpirationWindow();
		} catch (TimeSeriesException e) {
			logger.error("Error setting node local static variables for the CEP worker ", e);
		}
	}

	public void register() {
		System.out.println("Scheduling first event sweeper job for logical node = " + localLn + " in cluster " + localCluster);
		scheduler.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				logger.debug("Sweeping events at node " + localLn + " in cluster " + localCluster + " at time " + DateTime.now());
				System.out.println("Sweeping events at node " + localLn + " in cluster " + localCluster + " at time " + DateTime.now());
				cepService.sweepAndPurge(localCluster, localLn, TTL);
			}}, interval, interval, TimeUnit.SECONDS);
	}
	
	public boolean initialize() {
		this.wguid = UUID.randomUUID().toString();
		try {
			cepService = ServiceFactory.getCEPService();
			if (zk.exists(TsrConstants.ROOT_PATH + "/" + localCluster + "/eventsweepers/" + localLn, false) == null)
				zk.create(TsrConstants.ROOT_PATH + "/" + localCluster + "/eventsweepers/" + localLn, 
					wguid.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			else
				zk.setData(TsrConstants.ROOT_PATH + "/" + localCluster + "/eventsweepers/" + localLn, wguid.getBytes(), -1);
			System.out.println("Initialized event sweeper job with guid " + wguid + " for logical node = " + localLn + " in cluster " + localCluster);
			logger.debug("Initialized event sweeper job with guid " + wguid + " for logical node = " + localLn + " in cluster " + localCluster);
			return true;
		} catch (TimeSeriesException | KeeperException | InterruptedException e) {
			logger.error("Found error initializing CEP worker " + wguid + " at node " + localLn + " in cluster " + localCluster,e);
		}
		return false;
	}
	
 	public static void main(String[] args) throws Exception {
		EventSweeper sweeper = new EventSweeper();
		int retryMax = 60;
		int retryCount = 0;
		while (retryCount < retryMax) {
			retryCount++;
			try {
				boolean result = sweeper.initialize();
				if (result) {
					sweeper.register();
					while (true) {
						Thread.sleep(30000);
					}
				}  else {
					Thread.sleep(60000);
				}
			} catch (Exception e) {
				logger.error("Failed to initialize CEP worker after " + retryCount + " tries, retrying ...", e);
			}
			if (retryCount == retryMax) {
				logger.error("Unable to initialize CEP worker after " + retryMax + " tries, exiting ...");
				System.exit(3);
			}
		}
	}
}
