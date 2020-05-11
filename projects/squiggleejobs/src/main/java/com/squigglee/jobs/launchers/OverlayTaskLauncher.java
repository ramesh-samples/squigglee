// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.launchers;

import org.apache.log4j.Logger;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.jobs.task.OverlayViewPublisher;

public class OverlayTaskLauncher {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.launchers.OverlayTaskLauncher");
    private static OverlayTaskLauncher instance = null;
    protected boolean running = false;

    private OverlayTaskLauncher() { }
    
    public static OverlayTaskLauncher getInstance() {
        if (instance == null) {
            instance = new OverlayTaskLauncher();
        }
        return instance;
    }
    
    public static boolean isStarted() {
    	return getInstance().running;
    }
    
    public static void start() {
    	if (!getInstance().running) {
    		try {
    			logger.debug("Starting overlay service for local node " + LocalNodeProperties.getNodeLogicalNumber());
    			System.out.println("Starting overlay service for local node " + LocalNodeProperties.getNodeLogicalNumber());
    			new OverlayViewPublisher(LocalNodeProperties.getClusterName(), LocalNodeProperties.getNodeLogicalNumber(), null, 
    					LocalNodeProperties.getOverlayTimerInterval(), LocalNodeProperties.getOverlayVdbFile());
	    		instance.running = true;
			} catch (TimeSeriesException e) {
				logger.error("Error starting overlay task launcher", e);
			}
    	}
    }
    
    public static void stop() {
    	instance = null;
    }
    
    public static void main (String[] args) {
    	try {
   			start();
   			while (true) {
   				Thread.sleep(60000*5);
   			}
		} catch (Exception e) {
			System.out.println("Check logs for errors encountered during launching");
			logger.error("Error launching the overlay service ... " , e);
		}
    }
    
}
