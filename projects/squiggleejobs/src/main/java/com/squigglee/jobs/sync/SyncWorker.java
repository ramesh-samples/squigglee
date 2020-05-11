// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.sync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;

import com.squigglee.coord.interfaces.ICEPService;
import com.squigglee.coord.interfaces.IConfigService;
import com.squigglee.coord.interfaces.IWorker;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.coord.utility.ZooKeeperFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.SyncTask;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;

public class SyncWorker implements IWorker {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.sync.SyncWorker");
	protected static String localCluster = null;
	protected String syncTaskPath = null;
	protected static int localLn = 0;
	protected static int replicaOf = 0;
	protected static String addr = "127.0.0.1";
	protected static int taskCount = 1;
	protected static int maxClaims = 1;
	protected ICEPService syncService = null;
	protected IConfigService configService = null;
	protected SortedMap<Integer,SyncTask> runningTasks = null;
	protected ExecutorService executorService;
	protected static HandlerFactory handlerFactory = null;
	protected static ZooKeeper zk;
	protected String wguid = null;
	//protected String assignedGuid = null;
	//protected String workerInfo = null;
	private boolean spawner = true;
	List<String> claims = null;
	
	static {
		try {
			localCluster = LocalNodeProperties.getClusterName();
			localLn = LocalNodeProperties.getNodeLogicalNumber();
			replicaOf = LocalNodeProperties.isReplicaOf();
			addr = LocalNodeProperties.getNodeAddress();
			maxClaims = LocalNodeProperties.getMaxClaimsSync();
			taskCount = LocalNodeProperties.getBatchSyncTaskCount();
			zk = ZooKeeperFactory.getLocalZooKeeper();
		} catch (TimeSeriesException e) {
			logger.error("Error setting node local static variables for the sync worker ", e);
		}
	}
	
	private Watcher newTaskWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeChildrenChanged) {
				logger.debug("Received new task notification for worker with guid = " + wguid);
				processTasks();
				//doTasks();
			}
		}
	};
	
	private ChildrenCallback tasksChildrenCallback = new ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				processTasks();
				break;
			case OK:
				doTasks();
				break;
			default:
				break;
			}
		}
	};
	
	private void processTasks() {
		zk.getChildren(syncTaskPath, newTaskWatcher, tasksChildrenCallback, null);
	}
	
	private void doTasks() {
		checkClaim();
		
		SortedMap<Integer,SyncTask> tasks = syncService.getSyncTasks(localCluster, localLn, 
				configService.getMasterDataIds(localCluster, replicaOf, claims), taskCount, false);
		if (tasks == null || tasks.isEmpty())
			return;
		while (!tasks.isEmpty()) {
			for (int seq : tasks.keySet()) {
				SyncTask task = tasks.get(seq);
				if (!runningTasks.containsKey(seq) && runningTasks.size() <= taskCount) {
					runningTasks.put(seq, task);
					logger.debug("Running sync task " + task + " at location " + localLn + " in cluster " + localCluster);
					(new DataSyncTask(seq,task,runningTasks)).run();
				}
			}
			tasks = syncService.getSyncTasks(localCluster, localLn, configService.getMasterDataIds(localCluster, 
					localLn, claims), taskCount, false);
		}
	}

	public void register() {
		processTasks();
		doTasks();
	}
	
	public void checkClaim() {
		Set<String> guids = configService.getConfig(localCluster, replicaOf).keySet();
		System.out.println("Guids configured in system are " + guids);
		logger.debug("Guids configured in system are " + guids);
		//1) get rid of any prior claims no longer needed 
		for (String guid : claims)
			if (!guids.contains(guid)) {
				logger.info("Time Series id " + guid + " is no longer configured at node " + localLn + " in cluster " + localCluster + " ... removing");
				System.out.println("Time Series id " + guid + " is no longer configured at node " + localLn + " in cluster " + localCluster + " ... removing");
				removeClaim(guid);
				claims.remove(guid);
			}
		
		//2) claim more if limit is not yet reached 
		if (this.claims.size() < maxClaims)
			claim(guids);
		
		Set<String> currentClaims = syncService.getCurrentClaims(localCluster, localLn);
		logger.debug("Current claims at node " + localLn + " in cluster " + localCluster + " = " + currentClaims);
		//3) terminate if this worker is redundant
		if (currentClaims.size() > 1 && this.claims.isEmpty()) {
			System.out.println("This worker is not required as there no more claims and another bootstrap exists -- " + this.wguid);
			logger.info("This worker is not required as there no more claims and another bootstrap exists -- " + this.wguid);
			if (isSpawner())	//leave running for local unit testing 
				System.exit(0);	//one bootstrap sync worker is sufficient
		}

		//if (this.claims.isEmpty() && !guids.isEmpty())
		//	if (isSpawner())
		//		System.exit(0);		//scale down

		//4) create additional workers if needed
		if (this.claims.size() == maxClaims && currentClaims.size() < guids.size())
			if (isSpawner())
				spawnSyncWorkers(guids.size() - currentClaims.size());	//scale up
	}
	
	public void spawnSyncWorkers(int count) {
		
		for (int i=0; i<count; i++) {
			try {
				ProcessBuilder pb = new ProcessBuilder("java", "-cp", "squiggleejobs.jar", "-Dlog4j.configuration=file:log4j.properties", "-DtsrPropFile=./LocalNodeProperties.config", "-Dzookeeper.sasl.client=false", "com.squigglee.jobs.sync.SyncWorker");
				pb.inheritIO();
				Map<String, String> env = pb.environment();
				env.put("PATH", "/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/aws/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/usr/bin/java");
				pb.directory(new File(LocalNodeProperties.getServiceLocation()));
				Process proc = pb.start();
				//Process proc = Runtime.getRuntime().exec("sudo nohup java -cp ./squiggleejobs.jar -Dlog4j.configuration=file:log4j.properties -DtsrPropFile=./LocalNodeProperties.config -Dzookeeper.sasl.client=false com.squigglee.jobs.sync.SyncWorker &", 
				//	env, null);
				logger.info("Spawned new sync worker process " + proc + " from sync worker " + this.wguid);
				System.out.println("Spawned new sync worker process " + proc + " from sync worker " + this.wguid);
				
				//write this to a log file in a separate thread if needed 
				//InputStream error = proc.getErrorStream();
				//InputStreamReader isrerror = new InputStreamReader(error);
				//BufferedReader bre = new BufferedReader(isrerror);
				//String linee = null;
				//while ((linee = bre.readLine()) != null) {
				//        System.out.println(linee);
				//        logger.error(linee);
				//    }
				
			} catch (IOException | TimeSeriesException e) {
				logger.error("Error spawning new sync worker processes from sync worker " + this.wguid, e);
				System.out.println("Error spawning new sync worker processes from sync worker " + this.wguid);
			}
		}
	}
	
	//private void writeErrors(Process proc) {
	//	this.executorService.execute(command);
	//}
	
	public boolean claim() {
		Set<String> guids = configService.getConfig(localCluster, replicaOf).keySet();
		return claim(guids);
	}
	
	public boolean claim(Set<String> guids) {
		if (this.claims.size() >= maxClaims)
			return false;
		boolean claimed = false;
		try {
			String path = TsrConstants.ROOT_PATH + "/" + localCluster + "/syncworkers";
			for (String guid : guids) {	
				String workerinfo = localLn + ";" + guid;
				boolean priorClaim = false;
				for (String entry : zk.getChildren(path, false)) {
					if (workerinfo.equals(entry)) {
						priorClaim = true;
						break;	// has been claimed 
					}
				}
				if (priorClaim)
					continue;
				if (zk.exists(path + "/" + workerinfo, false) == null) { 
					zk.create(path + "/" + workerinfo, wguid.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
					String workerGuid = new String(zk.getData(path + "/" + workerinfo, false, null));
					if (workerGuid.equalsIgnoreCase(wguid)) {
						this.claims.add(guid);
						claimed = true;
						logger.info("Sync worker " + wguid + " claims time series id = " + guid + " at node " + localLn);
						System.out.println("Sync worker " + wguid + " claims time series id = " + guid + " at node " + localLn);
						if (this.claims.size() == maxClaims) {
							logger.debug("Sync worker " + wguid +  " has claimed the maximum possible guids at node " + localLn);
							System.out.println("Sync worker " + wguid +  " has claimed the maximum possible guids at node " + localLn);
							break;
						}
					}
				}
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error claiming guid and creating worker location",e);
		}
		return claimed;
	}
	
	public void removeClaim(String guid) {
		String path = TsrConstants.ROOT_PATH + "/" + localCluster + "/syncworkers";
		String workerinfo = localLn + ";" + guid;
		try {
			zk.delete(path + "/" + workerinfo, -1);
		} catch (InterruptedException | KeeperException e) {
			logger.error("Found error removing claim on guid " + guid + " from node " + localLn, e);
		}
	}
	
	public boolean initialize() {
		this.wguid = UUID.randomUUID().toString();
		try {
			syncTaskPath = TsrConstants.ROOT_PATH + "/" + LocalNodeProperties.getClusterName() + "/syncqueue";
			syncService = ServiceFactory.getCEPService();
			configService = ServiceFactory.getConfigurationService();
			runningTasks = new TreeMap<Integer,SyncTask>();
			executorService = Executors.newFixedThreadPool(1);
			claims = new ArrayList<String>();
			logger.debug("Created sync worker " + wguid + " at node " + localLn + " in cluster " + localCluster);
			checkClaim();
			return true;
		} catch (TimeSeriesException e) {
			logger.error("Found error initializing sync worker " + wguid + " at node " + localLn + " in cluster " + localCluster,e);
		}
		return false;
	}
	
	public static void main(String[] args) throws Exception {
		SyncWorker sw = new SyncWorker();
		
		int retryMax = 60;
		int retryCount = 0;
		while (retryCount < retryMax) {
			retryCount++;
			try {
				boolean result = sw.initialize();
				if (result) {
					sw.register();
					while (true) {
						Thread.sleep(30000);
					}
				}  else {
					Thread.sleep(60000);
				}
			} catch (Exception e) {
				logger.error("Failed to initialize sync worker after " + retryCount + " tries, retrying ...", e);
			}
			if (retryCount == retryMax) {
				logger.error("Unable to initialize sync worker after " + retryMax + " tries, exiting ...");
				System.exit(3);
			}
		}
		
		
	}

	public boolean isSpawner() {
		return spawner;
	}

	public void setSpawner(boolean spawner) {
		this.spawner = spawner;
	}

}
