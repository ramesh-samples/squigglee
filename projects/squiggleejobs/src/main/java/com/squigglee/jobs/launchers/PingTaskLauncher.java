// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.launchers;

import org.apache.log4j.Logger;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.jobs.task.PingTask;

public class PingTaskLauncher {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.launchers.PingTaskLauncher");
    private static PingTaskLauncher instance = null;
    protected boolean running = false;
    
    private PingTaskLauncher() { }
    
    public static PingTaskLauncher getInstance() {
        if (instance == null) {
            instance = new PingTaskLauncher();
        }
        return instance;
    }
    
    public static boolean isStarted() throws TimeSeriesException {
    	return getInstance().running;
    }
    
    public static void start() {
    	if (!getInstance().running) {
    		try {
    			logger.debug("Starting ping service for local node " + LocalNodeProperties.getNodeLogicalNumber());
    			System.out.println("Starting ping service for local node " + LocalNodeProperties.getNodeLogicalNumber());
    			new PingTask();
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
			logger.error("Error launching the ping service ... " , e);
		}
    }

}
