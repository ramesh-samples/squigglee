// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.event;

import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.joda.time.DateTime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.squigglee.coord.interfaces.ICEPService;
import com.squigglee.coord.interfaces.IConfigService;
import com.squigglee.coord.interfaces.IWorker;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.coord.utility.ZooKeeperFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.Query;
import com.squigglee.core.entity.Stream;
import com.squigglee.core.entity.SyncTask;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.interfaces.ICEPDataHandler;

public class EventWorker implements IWorker {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.sync.SyncWorker");
	protected static String localCluster = null;
	protected String syncTaskPath = null;
	protected static int localLn = 0;
	protected static int replicaOf = 0;
	protected static String addr = "127.0.0.1";
	protected static int taskCount = 1;
	protected SiddhiManager manager = null;
	protected IConfigService configService = null;
	protected ICEPService cepService = null;
	protected ICEPDataHandler cepHandler = null;
	protected SortedMap<Integer,SyncTask> runningTasks = null;
	protected ExecutorService executorService;
	protected static HandlerFactory handlerFactory = null;
	protected static ZooKeeper zk;
	protected String wguid = null;
	
	static {
		try {
			localCluster = LocalNodeProperties.getClusterName();
			localLn = LocalNodeProperties.getNodeLogicalNumber();
			replicaOf = LocalNodeProperties.isReplicaOf();
			addr = LocalNodeProperties.getNodeAddress();
			taskCount = LocalNodeProperties.getBatchSyncTaskCount();
			zk = ZooKeeperFactory.getLocalZooKeeper();
		} catch (TimeSeriesException e) {
			logger.error("Error setting node local static variables for the event worker ", e);
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
		Map<String,Map<Integer,List<Stream>>> streams = cepService.getStreams(localCluster, localLn);
		//List<Query> queries = cepService.getQueries(localCluster, localLn);
		updateManager(getStreamIds(streams));
		for (String cl : streams.keySet()) {
			for (int nodenum : streams.get(cl).keySet()) {
				for (Stream stream : streams.get(cl).get(nodenum)) {
					if (!stream.isDerived()) {
						List<String> guids = new ArrayList<String>();
				    	guids.add(stream.getName());
						List<Long> ids = configService.getMasterDataIds(cl, nodenum, guids);
						SortedMap<Integer,SyncTask> tasks = cepService.getSyncTasks(localCluster, localLn, ids, taskCount, true);
						if (tasks == null || tasks.isEmpty())
							return;
						while (!tasks.isEmpty()) {
							for (int seq : tasks.keySet()) {
								SyncTask task = tasks.get(seq);
								if (!runningTasks.containsKey(seq) && runningTasks.size() <= taskCount) {
									runningTasks.put(seq, task);
									logger.debug("Running CEP task " + task + " at location " + localLn + " in cluster " + localCluster);
									executorService.execute(new EventTask(seq, task, runningTasks, stream, manager));
									//(new EventTask(seq, task, runningTasks, stream, manager)).run();
								}
							}
							tasks = cepService.getSyncTasks(localCluster, localLn, ids, taskCount, true);
						}
					}
				}
			}
		}
	}
	
	private void updateManager(List<String> configuredStreams) {
		for (StreamDefinition def : manager.getStreamDefinitions()) {
			if (!configuredStreams.contains(def.getStreamId()))
				manager.removeStream(def.getStreamId());
		}
	}
	
	private List<String> getStreamIds(Map<String,Map<Integer,List<Stream>>> streams) {
		List<String> configuredStreams = new ArrayList<String>();
		for (String cl : streams.keySet()) {
			for (int nodenum : streams.get(cl).keySet()) {
				for (Stream stream : streams.get(cl).get(nodenum)) {
					if (!configuredStreams.contains(stream.getId()))
						configuredStreams.add(stream.getId());
				}
			}
		}
		return configuredStreams;
	}
	
/*	private List<String> getQueryIds(List<Query> queries) {
		List<String> configuredQueries = new ArrayList<String>();
		for (Query query : queries) {
			if (!configuredQueries.contains(query.getId()))
				configuredQueries.add(query.getId());
		}
		return configuredQueries;
	}*/

	public void register() {
		processTasks();
		doTasks();
	}
	
	public boolean initialize() {
		this.wguid = UUID.randomUUID().toString();
		try {
			if (zk.exists(TsrConstants.ROOT_PATH + "/" + localCluster + "/eventworkers/" + localLn, false) == null)
				zk.create(TsrConstants.ROOT_PATH + "/" + localCluster + "/eventworkers/" + localLn, 
					wguid.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			else
				zk.setData(TsrConstants.ROOT_PATH + "/" + localCluster + "/eventworkers/" + localLn, wguid.getBytes(), -1);
			syncTaskPath = TsrConstants.ROOT_PATH + "/" + LocalNodeProperties.getClusterName() + "/syncqueue";
			configService = ServiceFactory.getConfigurationService();
			cepService = ServiceFactory.getCEPService();
			cepHandler = HandlerFactory.getCEPDataHandler();
			runningTasks = new TreeMap<Integer,SyncTask>();
			executorService = Executors.newFixedThreadPool(1);
			manager = new SiddhiManager();
			addStreams();
			addQueries();
			addEventWriters();
			addStorageWriters();
			logger.debug("Created CEP worker " + wguid + " at node " + localLn + " in cluster " + localCluster);
			System.out.println("Created CEP worker " + wguid + " at node " + localLn + " in cluster " + localCluster);
			return true;
		} catch (TimeSeriesException | KeeperException | InterruptedException e) {
			logger.error("Found error initializing event worker " + wguid + " at node " + localLn + " in cluster " + localCluster,e);
		}
		return false;
	}
	
	private void addStreams() {
		Map<String,Map<Integer,List<Stream>>> streams = cepService.getStreams(localCluster, localLn);
		for (String cl : streams.keySet()) {
			for (int nodenum : streams.get(cl).keySet()) {
				for (Stream stream : streams.get(cl).get(nodenum)) {
					if (manager.getStreamDefinition(stream.getGuid()) == null) {
						String streamDef = "define stream " + stream.getId() + " (startts long, offset int, value double);";
						manager.defineStream(streamDef);
						System.out.println("Added stream definition to CEP engine -- " + streamDef);
					}
				}
			}
		}
	}
	
	private void addQueries() {
		for (Query query : cepService.getQueries(localCluster, localLn)) {
			manager.addQuery(query.getQueryText());
			System.out.println("Added query to CEP engine " + query);
		}
	}
	
	private void addEventWriters() throws TimeSeriesException {
		Map<String,Map<Integer,List<Stream>>> streams = cepService.getStreams(localCluster, localLn);
		for (String cl : streams.keySet()) {
			for (int nodenum : streams.get(cl).keySet()) {
				for (Stream stream : streams.get(cl).get(nodenum)) {
					//skip the raw time series events and any streams designated as non-persistent
					if (!stream.isDerived() || !stream.isPersisted())
						continue;
					StreamDefinition def = manager.getStreamDefinition(stream.getId());
					if (def.getStreamId().equalsIgnoreCase(stream.getId())) {
						manager.addCallback(stream.getId(), getEventWriter(localCluster, localLn, stream.getId(), this.cepService));
					}
				}
			}
		}
	}
	
	private void addStorageWriters() throws TimeSeriesException {
		Map<String,Map<Integer,List<Stream>>> streams = cepService.getStreams(localCluster, localLn);
		for (String cl : streams.keySet()) {
			for (int nodenum : streams.get(cl).keySet()) {
				for (Stream stream : streams.get(cl).get(nodenum)) {
					//skip the raw time series events and any streams designated as non-persistent
					if (!stream.isDerived() || !stream.isStored())
						continue;
					StreamDefinition def = manager.getStreamDefinition(stream.getId());
					if (def.getStreamId().equalsIgnoreCase(stream.getId())) {
						manager.addCallback(stream.getId(), getStorageWriter(localCluster, localLn, stream.getName(), this.cepHandler));
					}
				}
			}
		}
	}
	
	private StreamCallback getEventWriter(String cluster, int ln, String streamId, ICEPService cepService) throws TimeSeriesException {
		return (new StreamCallback() {
			private String streamId = null;
			private String cluster = null;
			private int ln = 0;
			private ICEPService cepService = null;
		     public void receive(Event[] events) {
		    	 logger.debug("Received events for stream id = " + this.streamId + " at node " + ln + " in cluster " + cluster + events);
		    	// EventPrinter.print(events);
	    	 	for (Event event : events) {	    	 		
	    	 		Object[] data = event.getData();
	    	 		cepService.writeEvent(localCluster, localLn, event.getStreamId(), 
	    	 				(long) Double.parseDouble(data[0].toString()), Integer.parseInt(data[1].toString()), Double.parseDouble(data[2].toString()),
	    	 				event.getTimeStamp());
	    	 	}
		     }
		     public StreamCallback initialize(String cluster, int ln, String streamId, ICEPService cepService) throws TimeSeriesException {
		    	 this.streamId = streamId;
		    	 this.ln = ln;
		    	 this.cluster = cluster;
		    	 this.cepService = cepService;
		    	 this.cepService.initializeStream(cluster, ln, streamId);
		    	 logger.info("Initialized new event writer for stream id = " + this.streamId + " at node " + ln + " in cluster " + cluster);
		    	 System.out.println("Initialized new event writer for stream id = " + this.streamId + " at node " + ln + " in cluster " + cluster);
		    	 return this;
		     }
		}).initialize(cluster, ln, streamId, cepService);
	}
	
	private StreamCallback getStorageWriter(String cluster, int ln, String streamName, ICEPDataHandler cepHandler) throws TimeSeriesException {
		return (new StreamCallback() {
			private String streamName = null;
			private String cluster = null;
			private int ln = 0;
			private ICEPDataHandler cepHandler = null;
			private List<MasterData> mdList = null;
			private Map<MasterData, SortedMap<Long, Object>> batch = new HashMap<MasterData, SortedMap<Long, Object>>();
			private Map<MasterData, Long> batchUpdateTime = new HashMap<MasterData, Long>();
			private int batchUpdateInterval = 5;	//seconds
			private int batchSize = 10000;	// 
		    public void receive(Event[] events) {
		    	//logger.debug("Received events for stream id = " + this.streamName + " at node " + ln + " in cluster " + cluster + events);
		    	if (events == null || events.length == 0)
		    		return;
		    	List<String> guids = new ArrayList<String>();
		    	guids.add(this.streamName);
				try {
		    	 	for (Event event : events) {
		    	 		Object[] data = event.getData();
		    	 		long startts = (long) Double.parseDouble(data[0].toString());
		    	 		MasterData masterData = null;
		    	 		for (MasterData md : mdList) {
		    	 			if (md.getStartts() == startts) {
		    	 				masterData = md;
		    	 				break;
		    	 			}
		    	 		}
		    	 		if (masterData == null)
		    	 			continue;	//if not configured skip storage 
		    	 		long offset = Integer.parseInt(data[1].toString());
		    	 		double value = Double.parseDouble(data[2].toString());
		    	 		
		    	 		if (!batch.containsKey(masterData))
		    	 			batch.put(masterData, new TreeMap<Long, Object>());
		    	 		if (!batch.get(masterData).containsKey(offset))
		    	 			batch.get(masterData).put(new Long(offset), new Double(value));
		    	 		
		    	 		for (MasterData md : batch.keySet())
		    	 			if (batch.get(md).size() >= batchSize || checkTime(md))
		    	 				postDataBatch(md);
		    	 	}
		    	 	
				} catch (TimeSeriesException e) {
					logger.error("Error persisting events for stream id = " + this.streamName + " at node " + this.ln + " in cluster " + this.cluster);
				}
		    }
		    private boolean checkTime(MasterData md) {
		    	if (batchUpdateTime.containsKey(md)) {
		    		if (batchUpdateTime.get(md) < DateTime.now().minusSeconds(batchUpdateInterval).getMillis())
		    			return true;
		    		else
		    			return false;
		    	} else {
		    		batchUpdateTime.put(md, DateTime.now().getMillis());
		    		return false;
		    	}
		    }
		    private synchronized void postDataBatch(MasterData md) throws TimeSeriesException {
	    	 	byte[] serialized = cepHandler.getSerializedData(md, batch.get(md));
	    	 	cepHandler.insertBulkData(serialized);
	    	 	logger.info("Inserted " + batch.get(md).size() + " events into time series with master data = " + md);
	    	 	System.out.println("Inserted " + batch.get(md).size() + " events into time series with master data = " + md);
	    	 	batch.get(md).clear();
	    	 	batchUpdateTime.put(md, DateTime.now().getMillis());
		    }
		    public StreamCallback initialize(String cluster, int ln, String streamName, ICEPDataHandler cepHandler) throws TimeSeriesException {
		    	 this.streamName = streamName;
		    	 this.ln = ln;
		    	 this.cluster = cluster;
		    	 this.cepHandler = cepHandler;
		    	 this.batchSize = LocalNodeProperties.getCEPEventStorageBatchSize();
		    	 this.batchUpdateInterval = LocalNodeProperties.getCEPEventStorageInterval();
		    	 this.mdList = cepHandler.getMasterData(cluster, ln, this.streamName, -1, -1);
		    	 logger.info("Initialized new storage writer for stream id = " + this.streamName);
		    	 System.out.println("Initialized new storage writer for stream id = " + this.streamName);
		    	 return this;
		    }
		}).initialize(cluster, ln, streamName, cepHandler);
	}
	
 	public static void main(String[] args) throws Exception {
		EventWorker sw = new EventWorker();
		
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
				logger.error("Failed to initialize CEP worker after " + retryCount + " tries, retrying ...", e);
			}
			if (retryCount == retryMax) {
				logger.error("Unable to initialize CEP worker after " + retryMax + " tries, exiting ...");
				System.exit(3);
			}
		}
	}
}
