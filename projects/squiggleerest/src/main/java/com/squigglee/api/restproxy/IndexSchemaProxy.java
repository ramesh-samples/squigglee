package com.squigglee.api.restproxy;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import com.squigglee.api.rest.RESTFactory;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IMasterDataHandler;

public class IndexSchemaProxy {
	private static Logger logger = Logger.getLogger("com.squigglee.api.restproxy.IndexSchemaProxy");
	protected static ExecutorService executorService = null;
	protected IMasterDataHandler mdHandler = null;
	protected String localCluster = null;
	protected int localLn = 0;
	protected String path = "/squiggleerestui";
	protected int port = 8080;
	protected String transport = "http";
	protected String address = "127.0.0.1";
	protected int connectionTimeoutMillis = 20000;
	protected int socketTimeoutMillis = 20000;
	
	public IndexSchemaProxy(IMasterDataHandler mdHandler, int localLn, String localCluster) {
		this.mdHandler = mdHandler;
		this.localLn = localLn;
		this.localCluster = localCluster;
	}
	
	public boolean createReplicaSetPatternIndexTables(String cluster, int ln, long id, String indexName) {
		if (!localCluster.equalsIgnoreCase(cluster) || TimeSeriesConfig.getLogicalNode(id) != ln)
			return false;
		boolean result = true;
		try {
			List<Integer> replicas = mdHandler.getReplicaSet(cluster, ln);
			executorService = Executors.newFixedThreadPool(replicas.size());
			CountDownLatch cdLatch = new CountDownLatch(replicas.size());
			int counter = 0;
			for (int replicaln : replicas) {
				counter++;
				if (replicaln == localLn) {
					createPatternIndexTables(cluster, ln, id, indexName);
					cdLatch.countDown();
				} else {
					executorService.execute(getCreateTask(counter, cluster, ln, id, indexName, cdLatch));
				}
			}
			cdLatch.await();
		} catch (TimeSeriesException e) {
			logger.error("Found error getting replica set for node " + ln + " in cluster " + cluster, e);
			result = false;
		} catch (InterruptedException e) {
			logger.error("Count down latch wait got interrupted", e);
			result = false;
		}
		return result;
	}
	
	public boolean deleteReplicaSetPatternIndexTables(String cluster, int ln, List<Long> idList, String indexName) {
		if (!localCluster.equalsIgnoreCase(cluster))
			return false;
		boolean result = true;
		try {
			List<Integer> replicas = mdHandler.getReplicaSet(cluster, ln);
			executorService = Executors.newFixedThreadPool(replicas.size());
			CountDownLatch cdLatch = new CountDownLatch(replicas.size());
			int counter = 0;
			for (int replicaln : replicas) {
				counter++;
				if (replicaln == localLn) {
					deletePatternIndexTables(cluster, ln, idList, indexName);
					cdLatch.countDown();
				} else {
					executorService.execute(getDeleteTask(counter, cluster, ln, idList, indexName, cdLatch));
				}
			}
			cdLatch.await();
		} catch (TimeSeriesException e) {
			logger.error("Found error getting replica set for node " + ln + " in cluster " + cluster, e);
			result = false;
		} catch (InterruptedException e) {
			logger.error("Count down latch wait got interrupted", e);
			result = false;
		}
		return result;
	}
	
	//data local execution only
	public boolean createPatternIndexTables(String cluster, int ln, long id, String indexName) throws TimeSeriesException {
		if (!localCluster.equalsIgnoreCase(cluster))
			return false;
		boolean result = true;
		if (ln == localLn) {
			mdHandler.createPatternIndexTables(cluster, id, indexName, null);
		}
		return result;
	}
	
	//data local execution only
	public boolean deletePatternIndexTables(String cluster, int ln, List<Long> idList, String indexName) throws TimeSeriesException {
		if (idList == null || ln != localLn || idList.size() == 0 || !localCluster.equalsIgnoreCase(cluster))
			return false;
		return mdHandler.deletePatternIndexTables(idList, indexName);
	}

	private Runnable getCreateTask(int tid, String cluster, int ln, long id, String indexName, CountDownLatch cdLatch) {
		return new Runnable() {
			int tid = 0;
			private String cluster = null;
			private int ln = 0;
			private long id = 0;
			private String indexName = null;
			private CountDownLatch cdLatch = null;
			@Override
			public void run() {
				System.out.println("Launching remote proxy pattern match request for thread # " + tid 
						+ " for cluster = " + cluster + " and ln = " + ln);
				try {
					NodeStatus nodeLocation = mdHandler.getLocation(cluster, ln);
					logger.debug("Location for remote data = " + nodeLocation.getAddress() + " for thread # " + tid);
					//boolean result = 
					RESTFactory.getIndexSchemaProxy(nodeLocation.getAddress()).createPatternIndexTablesJSON(cluster, ln, id, indexName);
				} catch (Exception e) {
					logger.error("Found error executing proxy pattern match request for thread # " + tid 
							+ " for cluster = " + cluster + " and ln = " + ln, e);
				} finally {
					cdLatch.countDown();
				}
			}
			public Runnable initialize(int tid, String cluster, int ln, long id, String indexName, CountDownLatch cdLatch) {
				this.tid = tid;
				this.cluster = cluster;
				this.ln = ln;
				this.id = id;
				this.indexName = indexName;
				this.cdLatch = cdLatch;
				return this;
			}
		}.initialize(tid, cluster, ln, id, indexName, cdLatch);
	}
	
	private Runnable getDeleteTask(int tid, String cluster, int ln, List<Long> idList, String indexName, CountDownLatch cdLatch) {
		return new Runnable() {
			int tid = 0;
			private String cluster = null;
			private int ln = 0;
			private List<Long> idList = null;
			private String indexName = null;
			private CountDownLatch cdLatch = null;
			@Override
			public void run() {
				System.out.println("Launching remote proxy pattern match request for thread # " + tid 
						+ " for cluster = " + cluster + " and ln = " + ln);
				try {
					NodeStatus nodeLocation = mdHandler.getLocation(cluster, ln);
					logger.debug("Location for remote data = " + nodeLocation.getAddress() + " for thread # " + tid);
					//boolean result = 
						RESTFactory.getIndexSchemaProxy(nodeLocation.getAddress()).deletePatternIndexTablesJSON(cluster, ln, idList, indexName);
				} catch (Exception e) {
					logger.error("Found error executing proxy pattern match request for thread # " + tid 
							+ " for cluster = " + cluster + " and ln = " + ln, e);
				} finally {
					cdLatch.countDown();
				}
			}
			public Runnable initialize(int tid, String cluster, int ln, List<Long> idList, String indexName, CountDownLatch cdLatch) {
				this.tid = tid;
				this.cluster = cluster;
				this.ln = ln;
				this.idList = idList;
				this.indexName = indexName;
				this.cdLatch = cdLatch;
				return this;
			}
		}.initialize(tid, cluster, ln, idList, indexName, cdLatch);
	}
}
