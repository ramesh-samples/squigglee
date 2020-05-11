// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.squigglee.cloud.interfaces.ICloudHandler;
import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.interfaces.IStatusService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.interfaces.IDataHandler;

public class ConfigurationTask extends TimerTask {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.task.ConfigurationTask");
	private IStatusService statusService = null;
	private IDataService dataService = null;
	private ICloudHandler cloudHandler = null;
	private int ln = 0;
	private int interval = 10;
	private Timer timer = null;
	public ConfigurationTask (ICloudHandler cloudHandler, IStatusService statusService, IDataService dataService, Timer timer, int interval) 
			throws TimeSeriesException {
		this.cloudHandler = cloudHandler;
		this.dataService = dataService;
		this.statusService = statusService;
		this.timer = timer;
		this.interval = interval;
		initialize();
	}
	
	@Override
	public void run() {
		try { 
			List<NodeStatus> clusterStatus = statusService.getClusterStatus(LocalNodeProperties.getClusterName());
			
			List<Map<String, String>> insertTopology = new ArrayList<Map<String, String>>();
			List<Map<String, String>> deleteTopology = new ArrayList<Map<String, String>>();
			Map<String,List<String>> configureTopology = new HashMap<String,List<String>>();
			Map<String,List<Integer>> restartTopology = new HashMap<String,List<Integer>>();
			Map<String,List<Integer>> refreshTopology = new HashMap<String,List<Integer>>();
			//boolean seedOperation = false;
			boolean added = false;
			boolean deleted = false;
			for (NodeStatus node : clusterStatus) {
				if (node.getAddress() != null && node.getAddress().equalsIgnoreCase("CREATE")) {			// new node queued for creation 
					Map<String, String> map = new HashMap<String, String>();
					map.put("cluster", node.getCluster());
					map.put("ln", "" + node.getLogicalNumber());
					map.put("dc",node.getDataCenter());
					map.put("stype", node.getStype());
					map.put("storage","" + node.getStorage());
					map.put("replicaOf","" + node.getReplicaOf());	 
					map.put("isSeed","" + node.isSeedNode());
					map.put("isBoot","" + node.isBootstrapNode());
					map.put("nm", node.getName());
					insertTopology.add(map);
					//if (node.isSeedNode())
					//	seedOperation = true;
				}
				if (node.getName() != null && node.getName().equalsIgnoreCase("DELETE")) {			// existing node queued for deletion 
					Map<String, String> map = new HashMap<String, String>();
					map.put("cluster", node.getCluster());
					map.put("ln", "" + node.getLogicalNumber());
					map.put("dc",node.getDataCenter());
					map.put("iid",node.getInstanceId());
					map.put("addr",node.getAddress());
					IDataHandler dataHandler = HandlerFactory.getDataHandler();
					dataHandler.deleteNodeData(node.getCluster(), node.getLogicalNumber());
					dataHandler.shutdown();
					deleteTopology.add(map);
					//if (node.isSeedNode())
					//	seedOperation = true;
				}
				if (node.getName() != null && node.getName().equalsIgnoreCase("RESTART")) {			// existing node queued for restart 
					if (!restartTopology.containsKey(node.getCluster()))
						restartTopology.put(node.getCluster(), new ArrayList<Integer>());
					if (!restartTopology.get(node.getCluster()).contains(node.getLogicalNumber()))
						restartTopology.get(node.getCluster()).add(node.getLogicalNumber());
				}
				if (node.getName() != null && node.getName().equalsIgnoreCase("REFRESH")) {			// existing node queued for refresh 
					if (!refreshTopology.containsKey(node.getCluster()))
						refreshTopology.put(node.getCluster(), new ArrayList<Integer>());
					if (!refreshTopology.get(node.getCluster()).contains(node.getLogicalNumber()))
						refreshTopology.get(node.getCluster()).add(node.getLogicalNumber());
				}
				if (node.getAddress() != null && node.getAddress().equalsIgnoreCase("CONFIGURE")) {
					if (!configureTopology.containsKey(node.getCluster()))
						configureTopology.put(node.getCluster(), new ArrayList<String>());
					if (!configureTopology.get(node.getCluster()).contains(node.getLogicalNumber() + ";" + node.getReplicaOf()))
						configureTopology.get(node.getCluster()).add(node.getLogicalNumber() + ";" + node.getReplicaOf());
				}
			}

			if (insertTopology.size() > 0) {
				added = doInserts(insertTopology);
				if (!added)
					throw new TimeSeriesException("Failed to add new node(s) to cluster: " + insertTopology);
				if (added) {
					for (Map<String,String> map : insertTopology)
						dataService.setNode(map.get("cluster"), Integer.parseInt(map.get("ln")), "CONFIGURE", map.get("dc"), 
							"TBD", Boolean.parseBoolean(map.get("isBoot")), Boolean.parseBoolean(map.get("isSeed")), map.get("nm"), 
							Integer.parseInt(map.get("replicaOf")), Integer.parseInt(map.get("storage")), map.get("stype"));
					
					for (Map<String,String> map : insertTopology)
						cloudHandler.configureNode(map.get("cluster"), Integer.parseInt(map.get("ln")), Integer.parseInt(map.get("replicaOf")));
				}
			} else if (deleteTopology.size() > 0) {
				//try {
				//	Thread.sleep(180000);	//sleep for 3 minutes to let cassandra adjust as all node data may have been deleted 
				//} catch (InterruptedException e) {
				//	logger.error("Error while waiting for deleted node traffic to settle for node " + this.ln,e);
				//}
				deleted = doDeletes(deleteTopology);
				if (!deleted)
					throw new TimeSeriesException("Failed to delete new node(s) from cluster: " + deleteTopology);
				//else {
					/*boolean reconfig = false;
					for (Map<String,String> map : deleteTopology) {
						for (NodeStatus node : clusterStatus) {
							if (map.get("cluster").equalsIgnoreCase(node.getCluster()) && map.get("ln").equalsIgnoreCase("" + node.getLogicalNumber()) 
									&& node.isSeedNode()) {
								reconfig = true;
								break;
							}
						}
						if (reconfig)
							break;*/
					//}
					//if (reconfig)
					//	cloudHandler.configureNode(deleteTopology.get(0).get("cluster"), 0, 0); // simply reconfigure the bootstrap node
					
					cloudHandler.restartCoordinationService(deleteTopology.get(0).get("cluster"), 0, 0);
				//}
			} else {
				if (configureTopology.size() > 0) {
					for (String cluster : configureTopology.keySet())
						for (String lns : configureTopology.get(cluster))
							cloudHandler.configureNode(cluster, Integer.parseInt(lns.split(";")[0]), Integer.parseInt(lns.split(";")[1]));
				}
				else {
					for (String cluster : restartTopology.keySet())
						for (int ln : restartTopology.get(cluster)) {
							if (configureTopology.containsKey(cluster) && configureTopology.get(cluster).contains(ln))
								continue;
							cloudHandler.startNode(cluster,ln);
							cloudHandler.startServicesAtNode(cluster, ln);
						}
					for (String cluster : refreshTopology.keySet())
						for (int ln : refreshTopology.get(cluster)) {
							if ( (configureTopology.containsKey(cluster) && configureTopology.get(cluster).contains(ln)) 
									|| (restartTopology.containsKey(cluster) && restartTopology.get(cluster).contains(ln)) )
								continue;
							cloudHandler.startServicesAtNode(cluster, ln);
						}
				}
			}
			
			//if (added || deleted)
			//if (deleted) {
/*				for (NodeStatus node : clusterStatus) {
					if (deleteTopology.contains(node.getLogicalNumber()))
						continue;
					dataService.setNode(LocalNodeProperties.getClusterName(), node.getLogicalNumber(), "CONFIGURE", node.getDataCenter(), 
						node.getInstanceId(), node.isBootstrapNode(), node.isSeedNode(), node.getName(), node.getReplicaOf(), node.getStorage()
						, node.getStype());
					break; 		//configuring one is enough to propagate state to all zookeepers 
				}*/
				//cloudHandler.restartCoordinationService(deleteTopology.get(0).get("cluster"), 0, 0);
			//}
		} catch (Exception tse) {
			logger.error("Failed to run configuration script for logical node = " + this.ln,tse);
		}
		
		//timer = new Timer(true);
		System.out.println("Scheduling next configuration job for logical node = " + ln);
		logger.debug("Scheduling next configuration job for logical node = " + ln);
		try {
			timer.schedule(new ConfigurationTask(this.cloudHandler, this.statusService, this.dataService, this.timer, this.interval), 1000*this.interval);
		} catch (Exception e) {
			logger.debug("FATAL: Failed to schedule next configuration job for logical node = " + ln);
		}
	}
	
	@Override
	public boolean cancel() {
		
		if (timer != null) {
			timer.cancel();
			return true;
		}
		else
			return false;
	}
	
	private boolean doInserts(List<Map<String, String>> insertTopology) throws TimeSeriesException {
		boolean added = false;
		//most robust to create these one at a time 
		//for (Map<String,String> serverRequest : insertTopology) {
			try {
				//List<Map<String,String>> one = new ArrayList<Map<String,String>>();
				//one.add(serverRequest);
				//cloudHandler.createTopology(one);
				cloudHandler.createTopology(insertTopology);
				//for (Map<String, String> map : one)
				//	dataService.setNode(LocalNodeProperties.getClusterName(), Integer.parseInt(map.get("ln")), "CONFIGURE", map.get("dc"), 
				//			"TBD", Boolean.parseBoolean(map.get("isBoot")), Boolean.parseBoolean(map.get("isSeed")), map.get("nm"), 
				//			Integer.parseInt(map.get("replicaOf")), Integer.parseInt(map.get("storage")), map.get("stype"));
				added = true;
			} catch (TimeSeriesException tse) {
				logger.error("Error while creating new node " + insertTopology + " ; retry to follow ...", tse);
			}
		//}
		return added;
	}
	
	private boolean doDeletes(List<Map<String,String>> deleteTopology) throws NumberFormatException, TimeSeriesException {
		boolean deleted = false;
		for (Map<String, String> map : deleteTopology) {
			cloudHandler.stopServicesAtNode(map.get("cluster"), Integer.parseInt(map.get("ln")));
			dataService.deleteNode(LocalNodeProperties.getClusterName(), Integer.parseInt(map.get("ln")));
		}
		
		//for (Map<String,String> serverRequest : deleteTopology) {
			try {
				//List<Map<String,String>> one = new ArrayList<Map<String,String>>();
				//one.add(serverRequest);
				//cloudHandler.deleteTopology(one);
				cloudHandler.deleteTopology(deleteTopology);
				deleted = true;
			} catch (TimeSeriesException tse) {
				logger.error("Error while deleting node " + deleteTopology + " ; retry to follow ...", tse);
			}
		//}
		return deleted;
	}
	
	private void initialize() throws TimeSeriesException {
		ln = LocalNodeProperties.getNodeLogicalNumber();
		this.dataService = ServiceFactory.getDataService();
		this.statusService = ServiceFactory.getStatusService();
		if (timer == null) {
			timer = new Timer(true);
			System.out.println("Initializing and scheduling first configuration job for logical node = " + ln);
			timer.schedule(this, 1000*this.interval);
		}
	}
	
}
