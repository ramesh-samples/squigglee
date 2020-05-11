// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.task;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.overlay.OverlayManager;

public class OverlayViewPublisher {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.task.OverlayViewPublisher");
	private static final ScheduledExecutorService scheduler =
		     Executors.newScheduledThreadPool(1);
	private int interval = 10;
	private String vdbFile = null;
	private String localCluster = null;
	private int localLn = 0;
	private Map<String,Map<String, Map<Integer, List<String>>>> overlayNetwork = null;
	
	public OverlayViewPublisher(String cluster, int ln, Map<String,Map<String, Map<Integer, List<String>>>> overlayNetwork, 
			int interval, String vdbFile) throws TimeSeriesException {
		this.localCluster = cluster;
		this.localLn = ln;
		this.overlayNetwork = overlayNetwork;
		this.interval = interval;
		this.vdbFile = vdbFile;
		initialize();
	}
	
	protected void initialize() throws TimeSeriesException {
		if (interval <= 0) {
			publishView();
			return;
		} else {
			scheduler.scheduleWithFixedDelay(new Runnable() {
				@Override
				public void run() {
					logger.debug("Pinging coordination services at node " + localLn + " in cluster " + localCluster + " at time " + DateTime.now());
					System.out.println("Pinging coordination services at node " + localLn + " in cluster " + localCluster + " at time " + DateTime.now());
					try {
						publishView();
					} catch (Exception tse) {
						logger.error("Failed to run overlay view update task",tse);
					} 
				}}, interval, interval, TimeUnit.SECONDS);
		}
	}
	
	protected void publishView() throws TimeSeriesException {
		IMasterDataHandler overlayHandler = HandlerFactory.getMasterDataHandler();
		Map<String,Map<String, Map<Integer, List<String>>>> newNetwork = overlayHandler.getOverlayNetwork();
		overlayHandler.shutdown();
		if (newNetwork == null || newNetwork.isEmpty()) {
			logger.debug("No overlay network to publish");
			System.out.println("No overlay network to publish");
			File file = new File(this.vdbFile);
			if (newNetwork.isEmpty() && file.exists())
				file.delete();
			//System.exit(0);
			this.overlayNetwork = newNetwork;
			return;
		}
		
		if (OverlayManager.changed(this.overlayNetwork, newNetwork) 
				|| OverlayManager.changed(newNetwork, this.overlayNetwork)) {
			try {
				String vdbContents = OverlayManager.buildOverlayVdb(newNetwork);
				PrintWriter out = new PrintWriter(this.vdbFile);
				logger.debug("Updating VDB since overlay network has changed");
				out.print(vdbContents);
				out.close();
			} catch (Exception ex) {
				logger.error("Exception publishing view", ex);
			}
			this.overlayNetwork = newNetwork;
		}
		else
		{
			logger.debug("Skipping vdb update since overlay network has not changed");
		}
	}
}
