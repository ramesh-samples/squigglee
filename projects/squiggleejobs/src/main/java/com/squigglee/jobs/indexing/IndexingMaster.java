// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.indexing;

import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.squigglee.coord.interfaces.IMaster;
import com.squigglee.coord.interfaces.ITaskService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.coord.utility.ZooKeeperFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.TimeSeriesException;

public class IndexingMaster implements IMaster {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.indexing.IndexingMaster");
	protected String mguid = null;
	protected static String masterPath = null;
	protected static String workerPath = null;
	protected static String taskPath = null;
	protected static String assignPath = null;
	protected static String localCluster = null;
	protected static int localLn = 0;
	protected ITaskService taskService = null;
	protected static int taskCount = 1;
	protected ZooKeeper zk;
	protected  boolean isLeader = false;

	static {
		try {
			localCluster = LocalNodeProperties.getClusterName();
			masterPath = TsrConstants.ROOT_PATH + "/" + localCluster + "/master";
			taskPath = TsrConstants.ROOT_PATH + "/" + localCluster + "/taskqueue";
			workerPath = TsrConstants.ROOT_PATH + "/" + localCluster + "/workers";
			assignPath = TsrConstants.ROOT_PATH + "/" + localCluster + "/assignedqueue";
			localLn = LocalNodeProperties.getNodeLogicalNumber();
		} catch (TimeSeriesException e) {
			logger.error("Error setting node local static variables for the master ", e);
		}
	}

	private Watcher masterExistsWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("Received master node change event " + event.getPath() + " of type " + event.getType());
			if (event.getType().equals(Event.EventType.NodeDeleted)) {
				assert(masterPath.equals(event.getPath()));
				if (initialize())
					runMasterTasks();
			}
		}
	};
	
	
	private Watcher workerChangeWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			if (event.getType().equals(Event.EventType.NodeChildrenChanged)) {
				getWorkers();
			}
		}
	};
	
	private ChildrenCallback workerChildrenCallback = new ChildrenCallback() {

		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getWorkers();
				break;
			case OK:
				getTasks();
				//System.out.println("Updated Workers");
				break;
			default:
				logger.error("Failed to get children ", KeeperException.create(Code.get(rc),path));
				getTasks();
				break;
			}
		}
	};
	
	
	private Watcher taskChangeWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			if (event.getType().equals(Event.EventType.NodeChildrenChanged)) {
				getTasks();
			}
		}
	};

	private ChildrenCallback tasksChildrenCallback = new ChildrenCallback() {

		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getTasks();
				break;
			case OK:
				//System.out.println("In method tasksChildrenCallback of class IndexingMaster");
				if (children != null && isLeader)
					taskService.assignTasks(taskCount);
				//System.out.println("Assigned Tasks");
				break;
			default:
				logger.error("Failed to get children ", KeeperException.create(Code.get(rc),path));
				break;
			}
		}
	};
	
	public boolean initialize() {
		if (this.mguid == null) {
			this.mguid = UUID.randomUUID().toString();
			logger.debug("Created Indexing Master " + this.mguid);
		}
		try {
			this.zk = ZooKeeperFactory.getLocalZooKeeper();
			taskService = ServiceFactory.getTaskService();
			zk.create(masterPath, ("" + localLn).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			int masterLn = Integer.parseInt(new String(zk.getData(masterPath, false, null)));
			if (masterLn == localLn) {
				isLeader = true;
				logger.info("Node " + masterLn + " is the current leader");
				System.out.println("Node " + masterLn + " is the current leader");
			} else
				isLeader = false;
			
		} catch (KeeperException | InterruptedException | TimeSeriesException e) {
			int masterLn = 0;
			try {
				masterLn = Integer.parseInt(new String(zk.getData(masterPath, false, null)));
				System.out.println("Node " + masterLn + " is the current leader");
				logger.info("Node " + masterLn + " is the current leader");
			} catch (NumberFormatException | KeeperException
					| InterruptedException e1) {
				logger.error("Failed to initialize master, relaunch & try again", e1);
				return false;
			}
			if (masterLn == localLn) {
				isLeader = true;
			} else
				isLeader = false;
		}
		masterExists();
		return true;
	}

	public void close() throws InterruptedException {
		if (this.zk != null)
			zk.close();
	}
	
	public void masterExists() {
		try {
			Stat stat = zk.exists(masterPath, masterExistsWatcher);
			if (stat != null)
				System.out.println("Leaving watch on path for master" + masterPath);
		} catch (InterruptedException | KeeperException e) {
			logger.error("Failed to check if master exists", e);
		}
	}
	
	private void getTasks() {
		zk.getChildren(taskPath, taskChangeWatcher, tasksChildrenCallback, null);
	}
	
	private void getWorkers() {
		zk.getChildren(workerPath, workerChangeWatcher, workerChildrenCallback, null);
	}
	
	public void runMasterTasks() {
		if (isLeader) {
			getWorkers();
			taskService.reassign();
			//getTasks();
		}
	}
	
	public static void main(String[] args) {
			IndexingMaster m = new IndexingMaster();
			int retryMax = 60;
			int retryCount = 0;
			while (retryCount < retryMax) {
				retryCount++;
				try {
					boolean result = m.initialize();
					if (result) {
						m.runMasterTasks();
						while (true) {
							Thread.sleep(30000);
						}
					}
					else {						
						Thread.sleep(60000);
					}
				} catch (Exception e) {
					logger.error("Failed to initialize indexing master after " + retryCount + " tries, retrying ...", e);
				}
				if (retryCount == retryMax) {
					logger.error("Unable to initialize indexing master after " + retryMax + " tries, exiting ...");
					System.exit(3);
				}
			}
	}

}
