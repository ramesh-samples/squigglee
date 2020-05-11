// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.launchers;

import org.apache.log4j.Logger;

import com.squigglee.cloud.ec2.EC2AnsibleCloudHandler;
import com.squigglee.cloud.interfaces.ICloudHandler;
import com.squigglee.coord.interfaces.ICoordService;
import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.interfaces.IStatusService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.interfaces.IStatusHandler;
import com.squigglee.jobs.task.ConfigurationTask;

public class ConfigurationTaskLauncher {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.launchers.ConfigurationTaskLauncher");
    private static ConfigurationTaskLauncher instance = null;
    protected static ICloudHandler cloudHandler = null;
    protected static IStatusHandler statusHandler = null;
    private static ICoordService coordService = null;
    private static IStatusService statusService = null;
    private static IDataService dataService = null;
    protected static IDataHandler dataHandler = null;
    protected boolean running = false;

    private ConfigurationTaskLauncher() { }
    
    public static ConfigurationTaskLauncher getInstance() {
        if (instance == null) {
            instance = new ConfigurationTaskLauncher();
        }
        return instance;
    }
    
    public static boolean isStarted() {
    	return getInstance().running;
    }
    
    public static void start() {
    	if (!getInstance().running) {
    		try {
    			logger.debug("Starting configuration service for local node " + LocalNodeProperties.getNodeLogicalNumber());
    			System.out.println("Starting configuration service for local node " + LocalNodeProperties.getNodeLogicalNumber());
    			int interval = LocalNodeProperties.getConfigurationServiceInterval();
    			cloudHandler = new EC2AnsibleCloudHandler();
    			statusService = ServiceFactory.getStatusService();
    			dataService = ServiceFactory.getDataService();
    			coordService = ServiceFactory.getCoordinationService();
    			new ConfigurationTask(cloudHandler, statusService, dataService, null, interval);
	    		instance.running = true;
	    		coordService.close();
			} catch (TimeSeriesException e) {
				logger.error("Error starting configuration task launcher", e);
			}
    	}
    }
    
    public static void stop() {
    	//if (tasks != null)
    	//	watcher.stop();
    	//try {
		//	coordService.close();
		//} catch (TimeSeriesException e) {
			// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
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
			logger.error("Error launching the cloud configuration service ... " , e);
		}
    }
}
