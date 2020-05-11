// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.indexing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

import com.squigglee.coord.interfaces.IConfigService;
import com.squigglee.coord.interfaces.ICoordService;
import com.squigglee.coord.interfaces.IIndexService;
import com.squigglee.coord.interfaces.ITaskService;
import com.squigglee.coord.interfaces.IWorker;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.coord.utility.ZooKeeperFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.IndexingTask;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesException;

public class IndexingWorker implements IWorker {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.indexing.IndexingWorker");
	protected static String localCluster = null;
	//protected String workerPath = null;
	protected String assignedTaskPath = null;
	protected String assignedQueueTaskPath = null;
	protected String queueTaskPath = null;
	protected static int localLn = 0;
	protected static int replicaOf = 0;
	protected static String addr = "127.0.0.1";
	protected SortedMap<Integer,IndexingTask> runningTasks = null;
	protected ExecutorService executorService;
	protected ZooKeeper zk;
	protected IndexType indexType;
	protected String wguid = null;
	//protected String workerInfo = null;
	protected List<String> claims = null;
	protected int maxClaims = 1;
	private boolean spawner = true;
	protected static int taskCount = 1;
	protected static int patternChunkSize = 0;
	protected static int patternMaxChunks = 0;
	protected static int sketchChunkSize = 0;
	protected static int sketchMaxChunks = 0;
	protected static ICoordService coordService = null;
	protected static IConfigService configService = null;
	protected static IIndexService indexService = null;
	
	static {
		try {
			localCluster = LocalNodeProperties.getClusterName();
			localLn = LocalNodeProperties.getNodeLogicalNumber();
			replicaOf = LocalNodeProperties.isReplicaOf();
			addr = LocalNodeProperties.getNodeAddress();
			taskCount = LocalNodeProperties.getBatchIndexingTaskCount();
			patternChunkSize = LocalNodeProperties.getIndexChunkSize();
			patternMaxChunks = LocalNodeProperties.getIndexNumChunks();
			sketchChunkSize = LocalNodeProperties.getSketchChunkSize();
			sketchMaxChunks = LocalNodeProperties.getSketchNumChunks();
			coordService = ServiceFactory.getCoordinationService();
			configService = ServiceFactory.getConfigurationService();
			indexService = ServiceFactory.getIndexService();
		} catch (TimeSeriesException e) {
			logger.error("Error setting node local static variables for the master ", e);
		}
	}
	
	public IndexingWorker(IndexType indexType) {
		this.indexType = indexType;
	}
	
	private Watcher newTaskWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeChildrenChanged) {
				logger.debug("Received new task notification for worker with guid = " + wguid);
				processTasks();
			}
		}
	};
	
	private ChildrenCallback tasksChildrenCallback = new ChildrenCallback() {
		public synchronized void processResult(int rc, String path, Object ctx,
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
	
	private void doTasks() {
		try {
			checkClaim();
			System.out.println("In method tasksChildrenCallback of class IndexingWorker of IndexType " + indexType);
			ITaskService taskService = ServiceFactory.getTaskService();
			IConfigService configService = ServiceFactory.getConfigurationService();
			if (this.claims.isEmpty())
				return;		//no index claimed yet
			SortedMap<Integer,IndexingTask> taskMap = taskService.getAssignedTasks(wguid, taskCount);
			if (taskMap == null || taskMap.size() == 0)
				return;
			while(taskMap != null && !taskMap.isEmpty()) {
				for (Integer seq : taskMap.keySet()) {
					if (!runningTasks.containsKey(seq) && runningTasks.size() <= taskCount) {
						IndexingTask task = taskMap.get(seq);
						assert task.getDestinationLn() == localLn;						
						MasterData md = configService.getConfig(task);
						if (md == null) {
							logger.error("IndexingMaster data does not exist for indexing task " + task);
							continue;
						}
						runningTasks.put(seq, task);
						logger.debug("Running task " + task + " at location " + localLn + " in cluster " + localCluster);
						//System.out.println("Starting task " + task + " at time " + DateTime.now());
						if (IndexType.valueOf(task.getIndexName().split("_")[0]).equals(IndexType.ptrn))
							//	executorService.execute(new PatternIndexingTask(seq, task, md, runningTasks, wguid, patternChunkSize, patternMaxChunks));
								(new PatternIndexingTask(seq, task, md, runningTasks, wguid, patternChunkSize, patternMaxChunks
										,coordService.getReplicaSet(localCluster, md.getId()).size(),(localLn-replicaOf))).run();
						if (IndexType.valueOf(task.getIndexName().split("_")[0]).equals(IndexType.skchCM))
						//	executorService.execute(new SketchIndexingTask(seq, task, md, runningTasks, wguid, sketchChunkSize, sketchMaxChunks));
							(new SketchIndexingTask(seq, task, md, runningTasks, wguid, sketchChunkSize, sketchMaxChunks)).run();
						if (IndexType.valueOf(task.getIndexName().split("_")[0]).equals(IndexType.skchEX))
						//	executorService.execute(new SketchIndexingTask(seq, task, md, runningTasks, wguid, sketchChunkSize, sketchMaxChunks));
							(new SketchIndexingTask(seq, task, md, runningTasks, wguid, sketchChunkSize, sketchMaxChunks)).run();
						//System.out.println("Completed task " + task + " at time " + DateTime.now());
					}
				}
				taskMap = taskService.getAssignedTasks(wguid, taskCount);
			}
		} catch (TimeSeriesException tse) {
			logger.error("Failed to process jobs ", tse);
			//System.exit(-1);
		}
	}
	
	private void processTasks() {
		zk.getChildren(queueTaskPath, newTaskWatcher, tasksChildrenCallback, null);
	}

	public void register() {
		processTasks();
	}
	
	public boolean claim() {
		List<String> guidIndexList = configService.getGuidIndexList(localCluster, replicaOf, this.indexType);
		return claim(guidIndexList);
	}
	
	public boolean claim(List<String> guidIndexList) {
		if (this.claims.size() >= this.maxClaims)
			return false;
		boolean claimed = false;
		String path = TsrConstants.ROOT_PATH + "/" + localCluster + "/workers";
		for (String guid : guidIndexList) {
			try {
				boolean priorClaim = false;
				String info = localLn + ";" + this.indexType + ";" + guid;
				for (String entry : zk.getChildren(path, false)) {
					if (info.equalsIgnoreCase(entry)) {
						priorClaim = true;		//another worker has claimed it
						break;
					}
				}
				if (priorClaim) {
					continue;
				}
				if (zk.exists(path + "/" + info, false) == null) {
					zk.create(path + "/" + info, this.wguid.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
					if (zk.exists(assignedTaskPath, false) == null)
						zk.create(assignedTaskPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					if (zk.exists(assignedQueueTaskPath, false) == null)
						zk.create(assignedQueueTaskPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					String workerGuid = new String(zk.getData(path + "/" + info, false, null));
					if (workerGuid.equalsIgnoreCase(this.wguid)) {
						this.claims.add(guid);
						logger.info("Indexing worker " + wguid + " claims time series id = " + guid + " at node " + localLn + " for index type = " + this.indexType);
						System.out.println("Indexing worker " + wguid + " claims time series id = " + guid + " at node " + localLn + " for index type = " + this.indexType);
						claimed = true;
						if (this.claims.size() == maxClaims) {
							logger.debug("Indexing worker " + wguid +  " of index type " + this.indexType + " has claimed the maximum possible guids at node " + localLn);
							System.out.println("Indexing worker " + wguid +  " of index type " + this.indexType + " has claimed the maximum possible guids at node " + localLn);
							break;
						}
					} else {
						logger.debug("deleting the assignment task paths as not needed since guid " + guid + " was not claimed");
						if (zk.exists(assignedTaskPath, false) != null)
							zk.delete(assignedTaskPath, -1);
						if (zk.exists(assignedQueueTaskPath, false) != null)
							zk.delete(assignedQueueTaskPath, -1);
					}
				}
			} catch (KeeperException e) {
				if (!e.code().equals(KeeperException.Code.NODEEXISTS)) {
					logger.error("Found error claiming guid and creating worker location for wguid = " + this.wguid,e);
					System.out.println("Found error claiming guid and creating worker location for wguid = " + this.wguid);
				}
			} catch (InterruptedException e) {
				logger.error("Found error claiming guid and creating worker location for wguid = " + this.wguid,e);
				System.out.println("Found error claiming guid and creating worker location for wguid = " + this.wguid);
			}
		}
		return claimed;
	}
	
	public void removeClaim(String guid) {
		String path = TsrConstants.ROOT_PATH + "/" + localCluster + "/workers";
		String info = localLn + ";" + this.indexType + ";" + guid;
		try {
			if (zk.exists(path + "/" + info, false) != null) 
				zk.delete(path + "/" + info, -1);
			logger.info("Guid " + guid + " is no longer configured at node " + localLn + " in cluster " + localCluster + " ... removing");
			System.out.println("Guid " + guid + " is no longer configured at node " + localLn + " in cluster " + localCluster + " ... removing");
		} catch (InterruptedException | KeeperException e) {
			logger.error("Found error removing claim on guid " + guid + " from node " + localLn, e);
		}
	}
	
	public void checkClaim() {
		List<String> guidIndexList = configService.getGuidIndexList(localCluster, replicaOf, this.indexType);
		System.out.println("Indexes configured in system for index type " + this.indexType + " at location " + localLn + " = " + guidIndexList);
		logger.debug("Indexes configured in system for index type " + this.indexType + " at location " + localLn + " = " + guidIndexList);
		//1)
		for (String guid : this.claims) {
			if (!guidIndexList.contains(guid)) {
				if (!guidIndexList.isEmpty())
					removeClaim(guid);
			}
		}
		
		//2)
		if (this.claims.size() < this.maxClaims)
			claim(guidIndexList);
		
		List<String> currentClaims = indexService.getCurrentClaims(localCluster, localLn, this.indexType);
		System.out.println("Current claims for index type " + this.indexType + " at location " + localLn + " = " + currentClaims);
		logger.debug("Current claims for index type " + this.indexType + " at location " + localLn + " = " + currentClaims);
		//3)
		if (currentClaims.size() > 1 && this.claims.isEmpty()) {
			System.out.println("This worker is not required as there no more guids to claim and another bootstrap exists -- " + this.wguid);
			logger.info("This worker is not required as there no more guids to claim and another bootstrap exists -- " + this.wguid);
			if (isSpawner())	//false leave running for local unit testing 
				System.exit(0);			//one bootstrap indexing worker is sufficient
		}
		
		//4)
		if (this.claims.size() == maxClaims && currentClaims.size() < guidIndexList.size())
			if (isSpawner())
				spawnIndexingWorkers(guidIndexList.size() - currentClaims.size());
	}
	
	public void spawnIndexingWorkers(int count) {
		for (int i=0; i<count; i++) {
			try {
				ProcessBuilder pb = new ProcessBuilder("java", "-cp", "squiggleejobs.jar", "-Dlog4j.configuration=file:log4j.properties", "-DtsrPropFile=./LocalNodeProperties.config", "-Dzookeeper.sasl.client=false", "com.squigglee.jobs.indexing.IndexingWorker", this.indexType.name());
				pb.inheritIO();
				Map<String, String> env = pb.environment();
				env.put("PATH", "/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/aws/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/usr/bin/java");
				pb.directory(new File(LocalNodeProperties.getServiceLocation()));
				Process proc = pb.start();
				//Process proc = Runtime.getRuntime().exec("sudo nohup java -cp ./squiggleejobs.jar -Dlog4j.configuration=file:log4j.properties -DtsrPropFile=./LocalNodeProperties.config -Dzookeeper.sasl.client=false com.squigglee.jobs.indexing.IndexingWorker &", 
				//	env, null);
				logger.info("Spawned new indexing worker process " + proc + " from indexing worker " + this.wguid);
				System.out.println("Spawned new indexing worker process " + proc + " from indexing worker " + this.wguid);
				
				//write this to a log file in a separate thread if needed 
				//InputStream error = proc.getErrorStream();
				//InputStreamReader isrerror = new InputStreamReader(error);
				//BufferedReader bre = new BufferedReader(isrerror);
				//String linee = null;
				//while ((linee = bre.readLine()) != null) {
				 //       System.out.println(linee);
				 //   }
			} catch (IOException | TimeSeriesException e) {
				logger.error("Error spawning new sync worker processes from sync worker " + this.wguid, e);
				System.out.println("Error spawning new sync worker processes from sync worker " + this.wguid);
			}
		}
	}
	
	public boolean initialize() {
		this.wguid = UUID.randomUUID().toString();
		try {
			if (this.indexType.equals(IndexType.ptrn))
				this.maxClaims = LocalNodeProperties.getMaxClaimsPattern();
			else 
				this.maxClaims = LocalNodeProperties.getMaxClaimsSketch();
				
			assignedTaskPath = TsrConstants.ROOT_PATH + "/" + LocalNodeProperties.getClusterName() + "/assigned/" + this.wguid;
			assignedQueueTaskPath = TsrConstants.ROOT_PATH + "/" + LocalNodeProperties.getClusterName() + "/assignedqueue/" + this.wguid;
			queueTaskPath = TsrConstants.ROOT_PATH + "/" + LocalNodeProperties.getClusterName() + "/taskqueue";
			this.zk = ZooKeeperFactory.getLocalZooKeeper();
			executorService = Executors.newFixedThreadPool(1);
			this.runningTasks = new TreeMap<Integer,IndexingTask>();
			this.claims = new ArrayList<String>();
			logger.debug("Created indexing worker " + wguid + " for index type " + this.indexType + " with max claims set to " + this.maxClaims + " at node " + localLn + " in cluster " + localCluster);
			checkClaim();
			return true;
		} catch (TimeSeriesException e) {
			logger.error("Found error initializing indexing worker for index type " + this.indexType + " at node " + localLn + " in cluster " + localCluster,e);
		}
		return false;
	}
	
	public static void main(String[] args) throws Exception {	
		IndexType it = IndexType.valueOf(args[0]);
		IndexingWorker w = new IndexingWorker(it);
		int retryMax = 60;
		int retryCount = 0;
		while (retryCount < retryMax) {
			retryCount++;
			try {
			boolean result = w.initialize();
			if (result) {
				w.register();
				while (true) {
					Thread.sleep(60000);
				}
			} else {
				Thread.sleep(60000);
			}
			} catch (Exception e) {
				logger.error("Failed to initialize indexing worker after " + retryCount + " tries, retrying ...", e);
			}
			if (retryCount == retryMax) {
				logger.error("Unable to initialize indexing worker after " + retryMax + " tries, exiting ...");
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

