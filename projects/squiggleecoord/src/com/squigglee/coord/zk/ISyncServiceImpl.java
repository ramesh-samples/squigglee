package com.squigglee.coord.zk;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.joda.time.DateTime;

import com.squigglee.coord.interfaces.ISyncService;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.SyncTask;

public abstract class ISyncServiceImpl extends IConfigServiceImpl implements ISyncService {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.ISyncServiceImpl");
	
	public void addSync(SyncTask task) {
		if (task == null)
			return;
		try {
			String entry = zk.create(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/syncs/", new byte[0], 
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);			
			storeTask(entry, task);
			String queueEntry = zk.create(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/syncqueue/", entry.getBytes(), 
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			task.setCurrentPath(queueEntry);
			System.out.println("Added sync task request " + task);
		} catch (KeeperException e) {
			logger.error("Error creating the next sequential task node for sync task " + task, e);
		} catch (InterruptedException e) {
			logger.error("Error creating the next sequential task node for sync task " + task, e);
		}
	}

	public void logSync(SyncTask task, int ln) {
		try {
			String storedTaskPath = new String(zk.getData(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/syncqueue/" + task.getCurrentPath(), false, null));
			//this.nodeOperator.createNode(storedTaskPath + "/synced/" + ln, new byte[0], CreateMode.PERSISTENT);
			if (this.zk.exists(storedTaskPath, false) != null) {
					this.zk.create(storedTaskPath + "/synced/" + ln, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			//skip in case a completed sync has already been deleted from another node 
			if (!e.code().equals(KeeperException.Code.NONODE) && !e.code().equals(KeeperException.Code.NODEEXISTS))
				logger.error("Found error fetching stored task path for task " + task,e);
		} catch (InterruptedException e) {
			logger.error("Found error fetching stored task path for task " + task,e);
		}
	}
	
	public abstract SortedMap<Integer,SyncTask> getSyncTasks(String cluster, int ln, List<Long> ids, int count, boolean isCEP);
	
	public Set<String> getCurrentClaims(String cluster, int nodeln) {
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/syncworkers";
		Set<String> claimList = new HashSet<String>();
		try {
			for (String entry : this.zk.getChildren(path, false)) {
				String[] tokens = entry.split(";");
				if (Integer.parseInt(tokens[0]) == nodeln)
					if (!claimList.contains(tokens[1]))
						claimList.add(tokens[1]);
			}
		} catch (KeeperException e) {
			logger.error("Error getting current claims for node " + nodeln + " in cluster " + cluster, e);
		} catch (InterruptedException e) { 
			logger.error("Error getting current claims for node " + nodeln + " in cluster " + cluster, e);
		}
		return claimList;
	}
	
	protected void deleteSyncTask(SyncTask task) throws KeeperException, InterruptedException {
		deleteRecursive(new String(zk.getData(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/syncqueue/" + task.getCurrentPath(), false, null)));
		deleteRecursive(TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/syncqueue/" + task.getCurrentPath());
		System.out.println("Deleted completed sync task " + task + " at time = " + DateTime.now());
	}
	
	protected void storeTask(String taskPath, SyncTask task) {
		/*
		//asynchronous
		this.nodeOperator.createNode(taskPath + "/synced", new byte[0], CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/cepsynced", new byte[0], CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/id", ("" + task.getId()).getBytes(), CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/start", ("" + task.getStartoffset()).getBytes(), CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/end", ("" + task.getEndoffset()).getBytes(), CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/operation", (task.getDataOperation().toString()).getBytes(), CreateMode.PERSISTENT);
		this.nodeOperator.createNode(taskPath + "/data", task.getData(), CreateMode.PERSISTENT);
		*/
		
		//synchronous
		try {
			this.zk.create(taskPath + "/synced", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/cepsynced", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/id", ("" + task.getId()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/start", ("" + task.getStartoffset()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/end", ("" + task.getEndoffset()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/operation", (task.getDataOperation().toString()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.zk.create(taskPath + "/data",task.getData(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			logger.error("Found storing sync task " + task,e);
		} catch (InterruptedException e) {
			logger.error("Found storing sync task " + task,e);
		}
		
	}
	
	protected boolean isSyncTaskValid(String cluster, long id) {
		boolean valid = true;
		MasterData md = this.getMasterData(cluster, id);
		if (md == null)
			valid = false;
		return valid;
	}
}
