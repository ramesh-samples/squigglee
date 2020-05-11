// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import com.squigglee.coord.interfaces.ITaskService;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.IndexingTask;
import com.squigglee.core.entity.MasterData;

public class ITaskServiceImpl extends IConfigServiceImpl implements ITaskService {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.ITaskServiceImpl");
	
	
	public SortedMap<Integer, IndexingTask> getTasks(int count) {
		//System.out.println("In method getTasks of class ITaskServiceImpl");
		SortedMap<Integer, String> map = new TreeMap<Integer, String>();
		SortedMap<Integer, IndexingTask> submap = new TreeMap<Integer, IndexingTask>();
		try {
			for (String cluster : zk.getChildren(TsrConstants.ROOT_PATH, false)) {
				String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/taskqueue";
				for (String seq : zk.getChildren(pathString, false)) {
					map.put(Integer.parseInt(seq), seq);	// get all the sequences 
				}
				for (int seq : map.keySet()) {
					String taskPathSeq = new String(zk.getData(TsrConstants.ROOT_PATH + "/" + cluster + "/taskqueue/" + map.get(seq), false, null));
					String taskPath = TsrConstants.ROOT_PATH + "/" + cluster + "/tasks/" + taskPathSeq;
					submap.put(seq, getStoredIndexingTask(cluster, taskPath, map.get(seq)));
					if (submap.size() == count)
						break;
				}
			}
		} catch (KeeperException e) {
			//if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Error getting tasks", e);
		} catch (InterruptedException e) {
			logger.error("Error getting tasks", e);
		}
		return submap;
	}
	
	public SortedMap<Integer, IndexingTask> getAssignedTasks(String wguid, int count) {
		System.out.println("Getting assigned tasks for worker " + wguid);
		SortedMap<Integer, String> map = new TreeMap<Integer, String>();
		SortedMap<Integer, IndexingTask> submap = new TreeMap<Integer, IndexingTask>();
		try {
			for (String cluster : zk.getChildren(TsrConstants.ROOT_PATH, false)) {
				String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/assignedqueue/" + wguid;
				for (String seq : zk.getChildren(pathString, false)) {
					map.put(Integer.parseInt(seq), seq);	// get all the assigned sequences 
				}
				for (int seq : map.keySet())  {
					String storedTaskSequence = new String(zk.getData(pathString + "/" + map.get(seq), false, null));
					String storedTaskPath = TsrConstants.ROOT_PATH + "/" + cluster + "/assigned/" + wguid + "/" + storedTaskSequence;
					IndexingTask storedTask = getStoredIndexingTask(cluster, storedTaskPath, map.get(seq));
					if (!isTaskValid(storedTask)) {
						deleteAssignedTask(storedTask, wguid);
						continue;
					}					
					submap.put(seq, storedTask);
					if (submap.size() == count)
						break;
				}
			}
		} catch (KeeperException e) {
			logger.error("Error getting assigned tasks for worker id = " + wguid, e);
		} catch (InterruptedException e) {
			logger.error("Error getting assigned tasks for worker id = " + wguid, e);
		}
		//printAssignedTasks(wguid, submap);
		return submap;
	}
	
	protected void printAssignedTasks(String wguid, Map<Integer, IndexingTask> map) {
		System.out.println("Assigned count of tasks for worker " + wguid + " = " + map.size());
		for (IndexingTask task : map.values())
			System.out.println(task);
	}
	
	public void addTask(IndexingTask request) {
		if (request == null)
			return;
		try {
			String entry = zk.create(TsrConstants.ROOT_PATH + "/" + request.getCluster()+ "/tasks/", new byte[0], 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			String seq = entry.split("/")[entry.split("/").length-1];
			storeTask(entry, request);
			zk.create(TsrConstants.ROOT_PATH + "/" + request.getCluster()+ "/taskqueue/", seq.getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			System.out.println("Added indexing task request " + request);
		} catch (KeeperException e) {
			logger.error("Error creating the next sequential task node for request " + request, e);
		} catch (InterruptedException e) {
			logger.error("Error creating the next sequential task node for request " + request, e);
		}
	}

	
	public void deleteTask(IndexingTask task) {
		if (task.getCurrentPath() != null) {
			try {
				String storedTaskPath = new String(zk.getData(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/taskqueue/" + task.getCurrentPath(), false, null));
				deleteRecursive(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/tasks/" + storedTaskPath);
				deleteRecursive(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/taskqueue/" + task.getCurrentPath());
			} catch (KeeperException e) {
				logger.error("Error deleting completed index request " + task, e);
			} catch (InterruptedException e) {
				logger.error("Error deleting completed index request " + task, e);
			}
		}
	}
	
	public void deleteAssignedTask(IndexingTask task, String wguid) {
		if (task.getCurrentPath() != null) {
			try {
				String storedTaskPath = new String(zk.getData(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/assignedqueue/" 
						+ wguid + "/"+ task.getCurrentPath(), false, null));
				deleteRecursive(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/assigned/" + wguid + "/"+ storedTaskPath);
				deleteRecursive(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/assignedqueue/" + wguid + "/" + task.getCurrentPath());
			} catch (KeeperException e) {
				logger.error("Error deleting completed index request " + task, e);
			} catch (InterruptedException e) {
				logger.error("Error deleting completed index request " + task, e);
			}
		}
	}

	public void assignTasks(int count) {
		SortedMap<Integer, IndexingTask> tasks = getTasks(count);
		int loopCount = 0;
		int maxLoopCount = 10;
		while (tasks != null && tasks.size() > 0) {
			loopCount++;
			if (loopCount == maxLoopCount) {
				logger.error("Unable to find worker(s) to assign task " + tasks);
				return;
			}
			printTasks(tasks);
			for (Integer seq : tasks.keySet()) {
				IndexingTask task = tasks.get(seq);
				
				if (!isTaskValid(task)) {
					deleteTask(task);
					continue;
				}
				
				if (assignTask(task))
					deleteTask(task);
			}
			reassign();
			tasks = getTasks(count);
		}
	}
	
	private void printTasks(SortedMap<Integer, IndexingTask> map) {
		System.out.println("Tasks available for assignment are = " + map.size());
		for (IndexingTask task : map.values())
			System.out.println(task);
	}
	
	private String getWorker(String cluster, String guid, IndexType it, int destinationLn) {
		String pathString = TsrConstants.ROOT_PATH + "/";
		List<String> candidates = new ArrayList<String>();
		try {
			for (String configuredCluster : zk.getChildren(TsrConstants.ROOT_PATH, false)) {
				if (!cluster.equalsIgnoreCase(configuredCluster))
					continue;
				for (String workerInfo : zk.getChildren(pathString + configuredCluster + "/workers", false)) {
					String wguid = new String(zk.getData(pathString + configuredCluster + "/workers/" + workerInfo, false, null));
					String[] workerData = workerInfo.split(";");	// workerln, index type, guid
					int workerLn = Integer.parseInt(workerData[0]);
					IndexType workerit = IndexType.valueOf(workerData[1]);
					if (workerData[2].equalsIgnoreCase(guid) && workerit.equals(it) && destinationLn == workerLn)
						candidates.add(wguid);
				}
			}
			if (!candidates.isEmpty())
				return candidates.get(rand.nextInt(candidates.size()));
		} catch (KeeperException e) {
			logger.error("Found error getting list of workers", e);
		} catch (InterruptedException e) {
			logger.error("Found error getting list of workers", e);
		}
		return null;
	}
	
	public Map<String,List<String>> getFailedWorkers() {
		Map<String, List<String>> failedWorkers = new ConcurrentHashMap<String, List<String>>();
		String pathString = TsrConstants.ROOT_PATH + "/";
		try {
			for (String cluster : zk.getChildren(TsrConstants.ROOT_PATH, false)) {
				
				for (String wguid : zk.getChildren(pathString + cluster + "/assigned", false)) {
					boolean failed = true;
					for (String workerInfo : zk.getChildren(pathString + cluster + "/workers", false)) {
						String currentWorkerGuid = new String(zk.getData(pathString + cluster + "/workers/" + workerInfo, false, null));
						if (wguid.equalsIgnoreCase(currentWorkerGuid)) {
							failed = false;
							break;
						}
					}
					
					if (failed) {
						if (!failedWorkers.containsKey(cluster))
							failedWorkers.put(cluster, Collections.synchronizedList(new ArrayList<String>()));
						if (!failedWorkers.get(cluster).contains(wguid))
							failedWorkers.get(cluster).add(wguid);
					}
				}
			}
		} catch (KeeperException e) {
			logger.error("Found error getting list of workers", e);
		} catch (InterruptedException e) {
			logger.error("Found error getting list of workers", e);
		}
		return failedWorkers;
	}

	private boolean assignTask(IndexingTask request) {
		IndexType it = IndexType.valueOf(request.getIndexName().split("_")[0]);
		MasterData md = getMasterData(request.getCluster(), request.getId());
		if (md == null)
			return false;
		String wguid = getWorker(request.getCluster(), md.getGuid(), it, request.getDestinationLn());
		if (wguid == null)
			return false;
		try {
			String entry = zk.create(TsrConstants.ROOT_PATH + "/" + request.getCluster() + "/assigned/" + wguid + "/", new byte[0], 
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			String seq = entry.split("/")[entry.split("/").length-1];
			storeTask( TsrConstants.ROOT_PATH + "/" + request.getCluster() + "/assigned/" + wguid + "/" + seq ,request);
			
			zk.create(TsrConstants.ROOT_PATH + "/" + request.getCluster() + "/assignedqueue/" + wguid + "/", seq.getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			System.out.println("Assigned indexing task request " + request + " to worker " + wguid);
			return true;
		} catch (KeeperException e) {
			logger.error("Error creating assignment sequence node for request " + request + " and worker " + wguid, e);
		} catch (InterruptedException e) {
			logger.error("Error creating assignment sequence node for request " + request + " and worker " + wguid, e);
		}
		return false;
	}
	
	public void reassign() {
		Map<String,List<String>> failedWorkers = getFailedWorkers();
		if (failedWorkers == null || failedWorkers.isEmpty())
			return;
			
		for (String cluster : failedWorkers.keySet()) {
			for (String wguid : failedWorkers.get(cluster)) {
				try {
					String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/assignedqueue/" + wguid;
					List<String> entries = this.zk.getChildren(pathString, false);
					if (entries != null && !entries.isEmpty()) {
						System.out.println("This worker has failed and tasks are being reassigned " + wguid);
						Map<Long,String> sortedEntries = new TreeMap<Long,String>();
						for (String entry : entries)
							sortedEntries.put(Long.parseLong(entry), entry);
						for (Long sseq : sortedEntries.keySet()) {
							String seq = sortedEntries.get(sseq);
							String taskSeq = new String(zk.getData(TsrConstants.ROOT_PATH + "/" + cluster + "/assignedqueue/" + wguid + "/" + seq, false, null));
							String taskPath = TsrConstants.ROOT_PATH + "/" + cluster + "/assigned/" + wguid + "/" + taskSeq;
							IndexingTask task = getStoredIndexingTask(cluster, taskPath, seq);
								if (assignTask(task)) 
									deleteAssignedTask(task, wguid);
						}
					}
					this.nodeOperator.deleteNode(TsrConstants.ROOT_PATH + "/" + cluster + "/assigned/" + wguid);
					this.nodeOperator.deleteNode(TsrConstants.ROOT_PATH + "/" + cluster + "/assignedqueue/" + wguid);
				} catch (KeeperException e) {
					logger.error("Error getting tasks to reassign for workerd id = " + wguid, e);
				} catch (InterruptedException e) {
					logger.error("Error getting tasks to reassign for workerd id = " + wguid, e);
				}
			}
		}
	}
	
	private IndexingTask getStoredIndexingTask(String cluster, String currentTaskPath, String seq) {
		try {
			int destinationLn = Integer.parseInt(new String(this.zk.getData(currentTaskPath + "/destinationLn", false, null)));
			long id = Long.parseLong(new String(this.zk.getData(currentTaskPath + "/id", false, null))); 
			String indexName =  new String(this.zk.getData(currentTaskPath + "/indexName", false, null));
			long startoffset = Long.parseLong(new String(this.zk.getData(currentTaskPath + "/start", false, null))); 
			long endoffset = Long.parseLong(new String(this.zk.getData(currentTaskPath + "/end", false, null)));
			CommandType operation = CommandType.valueOf(new String(this.zk.getData(currentTaskPath + "/operation", false, null)));
			byte[] priorData = this.zk.getData(currentTaskPath + "/priorData", false, null);
			IndexingTask task = new IndexingTask(cluster, destinationLn, id, startoffset, endoffset, indexName, operation);
			task.setPriorData(priorData);
			task.setCurrentPath(seq);
			return task;
		} catch (NumberFormatException e) {
			logger.error("Found error fetching stored task from path " + currentTaskPath, e);
		} catch (KeeperException e) {
			logger.error("Found error fetching stored task from path " + currentTaskPath, e);
		} catch (InterruptedException e) {
			logger.error("Found error fetching stored task from path " + currentTaskPath, e);
		}
		return null;
	}
	
	private void storeTask(String taskPath, IndexingTask task) {
		
		/*
		//asynchronous
		this.nodeOperator.createNode(taskPath + "/destinationLn", ("" + task.getDestinationLn()).getBytes(), CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/id", ("" + task.getId()).getBytes(), CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/indexName", (task.getIndexName()).getBytes(), CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/start", ("" + task.getStartoffset()).getBytes(), CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/end", ("" + task.getEndoffset()).getBytes(), CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/operation", (task.getDataOperation().toString()).getBytes(), CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/priorData", task.getPriorData(), CreateMode.PERSISTENT);
		*/
		
		//synchronous
		try {
			this.zk.create(taskPath + "/destinationLn", ("" + task.getDestinationLn()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/id", ("" + task.getId()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/indexName", (task.getIndexName()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/start", ("" + task.getStartoffset()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/end", ("" + task.getEndoffset()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/operation",(task.getDataOperation().toString()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/priorData",task.getPriorData()==null?(new byte[0]):(task.getPriorData()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			logger.error("Found storing indexing task " + task,e);
		} catch (InterruptedException e) {
			logger.error("Found storing indexing task " + task,e);
		}
	}

	private boolean isTaskValid(IndexingTask task) {
		boolean valid = true;
		MasterData md = this.getMasterData(task.getCluster(), task.getId());
		if (md == null)
			valid = false;
		else {
			String indexes = md.getIndexes();
			if (indexes == null || !indexes.toLowerCase().contains(task.getIndexName().toLowerCase()))
				valid = false;
		}
		return valid;
	}

}
