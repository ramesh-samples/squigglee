package com.squigglee.coord.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.squigglee.coord.interfaces.ICEPService;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.Query;
import com.squigglee.core.entity.Stream;
import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.SyncTask;

public class ICEPServiceImpl extends ISyncServiceImpl implements ICEPService {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.ICEPServiceImpl");
	
	@Override
	public List<Query> getQueries(String cluster, int ln) {
		List<Query> queries = new ArrayList<Query>();
		String nodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/eventconfig/queries/" + ln;
		try {
			for (String guid : this.zkov.getChildren(nodePath, false)) {
				queries.add(new Query(guid,
						new String(this.zkov.getData(nodePath + "/" + guid + "/id", false, null)),
						new String(this.zkov.getData(nodePath + "/" + guid + "/cluster", false, null)), 
						Integer.parseInt(new String(this.zkov.getData(nodePath + "/" + guid + "/ln", false, null))), 
						new String(this.zkov.getData(nodePath + "/" + guid + "/name", false, null)), 
						new String(this.zkov.getData(nodePath + "/" + guid + "/queryText", false, null))));
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error fetching configured streams for node " + ln + " in cluster " + cluster, e);
		}
		return queries;
	}

	@Override
	public Map<String,Map<Integer,List<Stream>>> getStreams(String cluster, int ln) {
		Map<String,Map<Integer,List<Stream>>> streams = new HashMap<String,Map<Integer,List<Stream>>>();
		String nodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/eventconfig/streams/" + ln;
		try {
			for (String streamGuid : this.zkov.getChildren(nodePath, false)) {
				String cl = new String(this.zkov.getData(nodePath + "/" + streamGuid + "/cluster", false, null));
				if (!streams.containsKey(cl))
					streams.put(cl, new HashMap<Integer,List<Stream>>());
				int nodenum = Integer.parseInt(new String(this.zkov.getData(nodePath + "/" + streamGuid + "/ln", false, null)));
				if (!streams.get(cl).containsKey(nodenum))
					streams.get(cl).put(nodenum, new ArrayList<Stream>());
				boolean derived = Boolean.parseBoolean(new String(this.zkov.getData(nodePath + "/" + streamGuid + "/isDerived", false, null)));
				boolean persisted = Boolean.parseBoolean(new String(this.zkov.getData(nodePath + "/" + streamGuid + "/isPersistent", false, null)));
				boolean stored = Boolean.parseBoolean(new String(this.zkov.getData(nodePath + "/" + streamGuid + "/isStored", false, null)));
				Stream stream = new Stream(new String(this.zkov.getData(nodePath + "/" + streamGuid + "/id", false, null)),
						streamGuid, cl, nodenum, 
						new String(this.zkov.getData(nodePath + "/" + streamGuid + "/name", false, null)), derived, persisted, stored);
				streams.get(cl).get(nodenum).add(stream);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error fetching configured streams for node " + ln + " in cluster " + cluster, e);
		}
		return streams;
	}
	
	@Override
	public Map<String,List<Integer>> getInterestedLocations(SyncTask task) {
		Map<String,List<Integer>> interestedLocations = new HashMap<String,List<Integer>>();
		
		String nodePath = TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/eventconfig/streams";
		try {
			for (String lnString : this.zkov.getChildren(nodePath, false)) {
				int ln = Integer.parseInt(lnString);
				if (zk.exists(nodePath + "/" + lnString, false) == null)
					continue;
				for (String guid : zk.getChildren(nodePath + "/" + lnString, false)) {
					String cluster = new String(zk.getData(nodePath + "/" + lnString + "/" + guid + "/cluster", false, null));
					boolean interested = Boolean.parseBoolean(new String(zk.getData(nodePath + "/" + lnString + "/" + guid + "/isStored", false, null)));
					if (interested && ln == getLogicalNode(task.getId())) {
						if (!interestedLocations.containsKey(cluster))
							interestedLocations.put(cluster, new ArrayList<Integer>());
						if (!interestedLocations.get(cluster).contains(ln))
							interestedLocations.get(cluster).add(ln);
					}
				}
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error fetching locations interested in sync task " + task, e);
		}
		System.out.println("Interested locations for task " + task + " are " + interestedLocations);
		return interestedLocations;
	}
	
/*	@Override
	public Map<String,Map<Integer,List<Stream>>> getStreams(String cluster, int ln, boolean isDerived) {
		Map<String,Map<Integer,List<Stream>>> streams = new HashMap<String,Map<Integer,List<Stream>>>();
		String nodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/eventconfig/streams/" + ln;
		try {
			for (String streamGuid : this.zkov.getChildren(nodePath, false)) {
				String cl = new String(this.zkov.getData(nodePath + "/cluster", false, null));
				if (!streams.containsKey(cl))
					streams.put(cl, new HashMap<Integer,List<Stream>>());
				int nodenum = Integer.parseInt(new String(this.zkov.getData(nodePath + "/ln", false, null)));
				if (!streams.get(cl).containsKey(nodenum))
					streams.get(cl).put(nodenum, new ArrayList<Stream>());
				boolean derived = Boolean.parseBoolean(new String(this.zkov.getData(nodePath + "/isDerived", false, null)));
				Stream stream = new Stream(streamGuid, cl, nodenum, 
						new String(this.zkov.getData(nodePath + "/name", false, null)), derived);
				if (derived == isDerived)
					streams.get(cl).get(nodenum).add(stream);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error fetching configured streams for node " + ln + " in cluster " + cluster, e);
		}
		return streams;
	}*/

	@Override
	public void addStream(String cluster, int ln, Stream stream) {
		String configNodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/eventconfig/streams/" + ln;
		this.ovNodeOperator.createNode(configNodePath + "/" + stream.getId(), new byte[0], CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + stream.getId() + "/id", stream.getId().getBytes(), CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + stream.getId() + "/cluster", stream.getCluster().getBytes(), CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + stream.getId() + "/ln", ("" + stream.getLn()).getBytes(), CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + stream.getId() + "/name", stream.getName().getBytes(), CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + stream.getId() + "/isDerived", ("" + stream.isDerived()).getBytes(), CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + stream.getId() + "/isPersistent", ("" + stream.isPersisted()).getBytes(), CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + stream.getId() + "/isStored", ("" + stream.isStored()).getBytes(), CreateMode.PERSISTENT);
		logger.info("Added stream " + stream + " to configuration at node " + ln + " in cluster " + cluster);
	}

	@Override
	public void addQuery(String cluster, int ln, Query query) {
		String configNodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/eventconfig/queries/" + ln;
		this.ovNodeOperator.createNode(configNodePath + "/" + query.getId(), new byte[0], CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + query.getId() + "/id", query.getId().getBytes(), CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + query.getId() + "/cluster", query.getCluster().getBytes(), CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + query.getId() + "/ln", ("" + query.getLn()).getBytes(), CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + query.getId() + "/name", query.getName().getBytes(), CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(configNodePath + "/" + query.getId() + "/queryText", query.getQueryText().getBytes(), CreateMode.PERSISTENT);
		logger.info("Added query " + query + " to configuration at node " + ln + " in cluster " + cluster);
	}
	
	@Override
	public void removeStream(String cluster, int ln, String streamId) {
		String nodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/eventconfig/streams/" + ln;
		String eventNodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/events/" + ln;
		try {
				boolean isDerived = Boolean.parseBoolean(new String(this.zkov.getData(nodePath + "/" + streamId + "/isDerived", false, null)));
				if (isDerived) {
					deleteRecursive(eventNodePath + "/" + streamId);
				}
				deleteRecursiveOverlay(nodePath + "/" + streamId);
				logger.info("Removed stream " + streamId + " from configuration at node " + ln + " in cluster " + cluster);
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error deleting stream " + streamId + " from node " + ln + " in cluster " + cluster,e);
		}
	}

	@Override
	public void removeQuery(String cluster, int ln, String queryId) {
		String nodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/eventconfig/queries/" + ln + "/" + queryId;
		deleteRecursiveOverlay(nodePath + "/" + queryId);
		logger.info("Removed query " + queryId + " from configuration at node " + ln + " in cluster " + cluster);
	}

	@Override
	public void logCEPSync(SyncTask task, String cluster, int ln) {
		try {
			String storedTaskPath = new String(zk.getData(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/syncqueue/" + task.getCurrentPath(), false, null));
			//this.nodeOperator.createNode(storedTaskPath + "/synced/" + ln, new byte[0], CreateMode.PERSISTENT);
			
			if (this.zk.exists(storedTaskPath, false) != null) {
				this.nodeOperator.createNode(storedTaskPath + "/cepsynced/" + cluster, new byte[0], CreateMode.PERSISTENT);
				this.nodeOperator.createNode(storedTaskPath + "/cepsynced/" + cluster + "/" + ln, new byte[0], CreateMode.PERSISTENT);
				System.out.println("Logged CEP sync for path " + storedTaskPath + "/cepsynced/" + cluster + "/" + ln);
			}
		} catch (KeeperException e) {
			//skip in case a completed sync has already been deleted from another node 
			if (!e.code().equals(KeeperException.Code.NONODE) && e.code().equals(KeeperException.Code.NODEEXISTS))
				logger.error("Found error logging CEP sync for task " + task, e);
		} catch (InterruptedException e) {
			logger.error("Found error logging CEP sync for task " + task, e);
		}
	}
	
	@Override
	public void writeEvent(String cluster, int ln, String streamId, long startts, int offset, Object value, long eventTime) {
		try {
			String eventNodePath = this.zk.create(TsrConstants.ROOT_PATH + "/" + cluster + "/events/" + ln + "/" + streamId + "/" , 
					new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			this.nodeOperator.createNode(eventNodePath + "/eventTime", ("" + eventTime).getBytes(), CreateMode.PERSISTENT);
			this.nodeOperator.createNode(eventNodePath + "/startts", ("" + startts).getBytes(), CreateMode.PERSISTENT);
			this.nodeOperator.createNode(eventNodePath + "/offset", ("" + offset).getBytes(), CreateMode.PERSISTENT);
			byte[] valArray = null;
			if (value != null) {
				if (value instanceof byte[])
					valArray = (byte[]) value;
				else
					valArray = value.toString().getBytes();
			} else
				valArray = new byte[0];
				
			this.nodeOperator.createNode(eventNodePath + "/value", valArray, CreateMode.PERSISTENT);
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error writing event for stream id " + streamId + " startts " + startts + " offset " + offset 
					+ " value " + value + " for event time " + new DateTime(eventTime) + " at node " + ln + " in cluster " + cluster, e);
		}
	}
	
	@Override
	public void initializeStream(String cluster, int ln, String streamId) {
		String eventNodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/events/" + ln + "/" + streamId;
		try {
			this.zk.create(eventNodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error initializing stream id = " + streamId + " at node " + ln + " in cluster " + cluster,e);
		}
	}
	
	public List<com.squigglee.core.entity.Event> getEvents(String cluster, int ln, String streamId, int count, boolean last) {
		String eventNodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/events/" + ln + "/" + streamId;
		List<com.squigglee.core.entity.Event> events = new ArrayList<com.squigglee.core.entity.Event>();
		SortedMap<Long, com.squigglee.core.entity.Event> eventMap = new TreeMap<Long, com.squigglee.core.entity.Event>();
		if (last)
			eventMap = new TreeMap<Long, com.squigglee.core.entity.Event>(Collections.reverseOrder());
		try {
			for (String entry : this.zk.getChildren(eventNodePath, false)) {
				long eventTime = Long.parseLong(new String(this.zk.getData(eventNodePath + "/" + entry + "/eventTime", false, null)));
				long startts = Long.parseLong(new String(this.zk.getData(eventNodePath + "/" + entry + "/startts", false, null)));
				int offset = Integer.parseInt(new String(this.zk.getData(eventNodePath + "/" + entry + "/offset", false, null)));
				String value = new String(this.zk.getData(eventNodePath + "/" + entry + "/value", false, null));
				com.squigglee.core.entity.Event cepEvent = new com.squigglee.core.entity.Event(streamId, startts, offset, value, eventTime);
				if (!eventMap.containsKey(eventTime))
					eventMap.put(eventTime, cepEvent);					
			}
			for (com.squigglee.core.entity.Event event : eventMap.values()) {
				events.add(event);
				if (events.size() == count)
					return events;
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error fetching " + (last?"last ":"first ") + count + " events for stream id = " 
					+ streamId + " at node " + ln + " in cluster " + cluster, e);
		}
		return events;
	}
	
	public void sweepAndPurge(String cluster, int ln, int TTL) {
		Map<String,Map<Integer,List<Stream>>> streams = getStreams(cluster, ln);
		for (String cl : streams.keySet()) {
			for (int nodenum : streams.get(cl).keySet()) {
				for (Stream stream : streams.get(cl).get(nodenum)) {
					String eventNodePath = TsrConstants.ROOT_PATH + "/" + cluster + "/events/" + ln + "/" + stream.getId();
					try {
						if (zk.exists(eventNodePath, false) == null)
							continue;
						for (String entry : this.zk.getChildren(eventNodePath, false)) {
							DateTime eventTime = new DateTime(Long.parseLong(new String(this.zk.getData(eventNodePath + "/" + entry + "/eventTime", false, null)))
								, DateTimeZone.UTC);
							if (eventTime.isBefore(DateTime.now(DateTimeZone.UTC).minusSeconds(TTL).getMillis())) {
								deleteRecursive(eventNodePath + "/" + entry);
								//logger.debug("Deleted event at path " + eventNodePath + "/" + entry);
								//System.out.println("Deleted event at path " + eventNodePath + "/" + entry);
							}
						}
					} catch (NumberFormatException | KeeperException | InterruptedException e) {
						logger.error("Found error sweeping events at node " + ln + " in cluster " + cluster, e);
					}
				}
			}
		}
	}
	
	public SortedMap<Integer,SyncTask> getSyncTasks(String cluster, int ln, List<Long> ids, int count, boolean isCEP) {
		//Map<String,List<Integer>> configuredStreams = 
		SortedMap<Integer,String> nodeTasks = new TreeMap<Integer,String>();
		SortedMap<Integer, SyncTask> submap = new TreeMap<Integer, SyncTask>();
		if (ids == null || ids.size() == 0)
			return submap;
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/syncqueue";
		try {
			for (String entry : zk.getChildren(path, false)) {
				nodeTasks.put(Integer.parseInt(entry), entry);
			}
			for (int sequence : nodeTasks.keySet()) {
				try {
					String storedTaskPath = new String(zk.getData(path + "/" + nodeTasks.get(sequence), false, null));
					long id = Long.parseLong(new String(zk.getData(storedTaskPath + "/id", false, null)));					
					
					if ( !(getReplicaSet(cluster,id).contains(new Integer(ln)) && ids.contains(new Long(id))) )
						continue;	//skip sync tasks not part of this replica set & not related to assigned guid
					
					if (zk.exists(storedTaskPath + "/syncs/" + ln, false) != null)
						continue;		//this task already synced for this local node
					
					long start = Long.parseLong(new String(zk.getData(storedTaskPath + "/start", false, null)));
					long end = Long.parseLong(new String(zk.getData(storedTaskPath + "/end", false, null)));
					CommandType operation = CommandType.valueOf(new String(zk.getData(storedTaskPath + "/operation", false, null)));
					byte[] data = zk.getData(storedTaskPath + "/data", false, null);
					SyncTask syncTask = new SyncTask(cluster, id, start, end, operation, data);
					syncTask.setCurrentPath(nodeTasks.get(sequence));	
					
					if ( ( syncCompleted(syncTask) && cepSyncCompleted(syncTask) ) || !isSyncTaskValid(cluster, id) ) {
						deleteSyncTask(syncTask);
						continue;
					} else {
						if (isCEP)
							if (!cepSyncCompletedAtNode(syncTask, cluster, ln))
								submap.put(sequence, syncTask);

						if (!isCEP)
							if (!syncCompletedAtNode(syncTask, cluster, ln))
								submap.put(sequence, syncTask);
						
					}
					//System.out.println("submap = " + submap);
					if (submap.size() == count)
						break;
				} catch (Exception ex) {
					// do nothing as task may have been deleted following completion by another thread or another process after completion 
				}
			}
		} catch (KeeperException e) {
			logger.error("Found error fetching pending sync tasks for logical node " + ln,e);
		} catch (InterruptedException e) {
			logger.error("Found error fetching pending sync tasks for logical node " + ln,e);
		}
		return submap;
	}

	protected boolean syncCompleted(SyncTask task) {
		try {
			String storedTaskPath = new String(zk.getData(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/syncqueue/" + task.getCurrentPath(), false, null));
			for (Integer nodenum : getReplicaSet(task.getCluster(), task.getId())) {
				if (zk.exists(storedTaskPath + "/synced" + "/" + nodenum, false) == null) {
					return false;
				}
			}
		} catch (KeeperException e) {
			logger.error("Found error getting task sync status for task " + task,e);
		} catch (InterruptedException e) {
			logger.error("Found error getting task sync status for task " + task,e);
		}
		return true;
	}
	
	protected boolean syncCompletedAtNode(SyncTask task, String cluster, int ln) {
		try {
			String storedTaskPath = new String(zk.getData(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/syncqueue/" + task.getCurrentPath(), false, null));
			return zk.exists(storedTaskPath + "/synced" + "/" + ln, false) != null;
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error getting task sync status for task " + task,e);
		}
		return false;
	}
	
	protected boolean cepSyncCompletedAtNode(SyncTask task, String cluster, int ln) {
		try {
			String storedTaskPath = new String(zk.getData(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/syncqueue/" + task.getCurrentPath(), false, null));
			return zk.exists(storedTaskPath + "/cepsynced" + "/" + cluster + "/" + ln, false) != null;
		} catch (KeeperException | InterruptedException e) {
			logger.error("Found error getting task sync status for task " + task,e);
		}
		return false;
	}
	
	public boolean cepSyncCompleted(SyncTask task) {
		try {
			System.out.println("Checking if CEP sync completed for task " + task);
			String storedTaskPath = new String(zk.getData(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/syncqueue/" + task.getCurrentPath(), false, null));
			Map<String,List<Integer>> interestedLocations = getInterestedLocations(task);
			for (String cluster : interestedLocations.keySet()) {
				for (int ln : interestedLocations.get(cluster)) {
					if (zk.exists(storedTaskPath + "/cepsynced" + "/" + cluster + "/" + ln, false) == null) {
						System.out.println("CEP sync not completed for task " + task);
						return false;
					}
				}
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting task sync status for task " + task,e);
		} catch (InterruptedException e) {
			logger.error("Found error getting task sync status for task " + task,e);
		}
		System.out.println("CEP sync completed for task " + task);
		return true;
	}
}
