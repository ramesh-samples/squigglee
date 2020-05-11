package com.squigglee.coord.zk;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.StatusType;
import com.squigglee.core.entity.TimeSeriesException;

public class IDataServiceImpl extends ICoordServiceImpl implements IDataService {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.IDataServiceImpl");
	
	public void setNode(String cluster, int ln, String addr, String dc, String iid,	boolean isBoot, 
			boolean isSeed, String nm, int replicaOf, int storage, String stype) throws TimeSeriesException {
		//Important:
		//Local zk instances are replica set specific --> /data, /entitle, /tsconfig, /status, & /patterns are shared across the cluster 
		//Overlay zk instances are cluster specific --> workers, indexes, queues, tasks, data syncs, assignments are all replica set specific  
		try {
			CreateMode mode = CreateMode.PERSISTENT;
			String path = TsrConstants.ROOT_PATH + "/" + cluster;
			if (zkov.exists(path + "/data" + "/" + ln, false) == null)
				zkov.create(path + "/data" + "/" + ln, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			if (zkov.exists(path + "/tsconfig" + "/" + ln, false) == null)
				zkov.create(path + "/tsconfig" + "/" + ln, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			if (zkov.exists(path + "/eventconfig/streams" + "/" + ln, false) == null)
				zkov.create(path + "/eventconfig/streams" + "/" + ln, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			if (zkov.exists(path + "/eventconfig/queries" + "/" + ln, false) == null)
				zkov.create(path + "/eventconfig/queries" + "/" + ln, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			if (zkov.exists(path + "/status" + "/" + ln, false) == null)
				zkov.create(path + "/status" + "/" + ln, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			if (zkov.exists(path + "/entitle" + "/" + ln, false) == null)
				zkov.create(path + "/entitle" + "/" + ln, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			if (zkov.exists(path + "/data/" + ln + "/addr", false) == null)
				zkov.create(path + "/data/" + ln + "/addr", addr.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			else
				this.ovNodeOperator.setData(path + "/data/" + ln + "/addr", addr.getBytes());
			
			if (zkov.exists(path + "/data/" + ln + "/dc", false) == null)
				zkov.create(path + "/data/" + ln + "/dc", dc.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			else
				zkov.setData(path + "/data/" + ln + "/dc", dc.getBytes(), -1);
			
			if (zkov.exists(path + "/data/" + ln + "/iid", false) == null)
				zkov.create(path + "/data/" + ln + "/iid", iid.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			else
				zkov.setData(path + "/data/" + ln + "/iid", iid.getBytes(), -1);
			
			if (zkov.exists(path + "/data/" + ln + "/nm", false) == null)
				zkov.create(path + "/data/" + ln + "/nm", nm.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			else
				zkov.setData(path + "/data/" + ln + "/nm", nm.getBytes(), -1);
			
			if (zkov.exists(path + "/data/" + ln + "/isboot", false) == null)
				zkov.create(path + "/data/" + ln + "/isboot", (isBoot?"1":"0").getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			else
				zkov.setData(path + "/data/" + ln + "/isboot", (isBoot?"1":"0").getBytes(), -1);
			
			if (zkov.exists(path + "/data/" + ln + "/isseed", false) == null)
				zkov.create(path + "/data/" + ln + "/isseed", (isSeed?"1":"0").getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			else
				zkov.setData(path + "/data/" + ln + "/isseed", (isSeed?"1":"0").getBytes(), -1);
			
			if (zkov.exists(path + "/data/" + ln + "/storage", false) == null)
				zkov.create(path + "/data/" + ln + "/storage", ("" + storage).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			else
				zkov.setData(path + "/data/" + ln + "/storage", ("" + storage).getBytes(), -1);
			
			if (zkov.exists(path + "/data/" + ln + "/replicaof", false) == null)
				zkov.create(path + "/data/" + ln + "/replicaof", ("" + replicaOf).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			else
				zkov.setData(path + "/data/" + ln + "/replicaof", ("" + replicaOf).getBytes(), -1);
			
			if (zkov.exists(path + "/data/" + ln + "/stype", false) == null)
				zkov.create(path + "/data/" + ln + "/stype", stype.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			else
				zkov.setData(path + "/data/" + ln + "/stype", stype.getBytes(), -1);
			
			//NODE, OVERLAY, CONFIGURATION
			for (StatusType st : StatusType.values()) {
				if (zkov.exists(path + "/status/" + ln + "/" + st, false) == null)
					zkov.create(path + "/status/" + ln + "/" + st, 
						("" + DateTime.now(DateTimeZone.UTC).minusSeconds(this.TTL).getMillis()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
				else
					this.ovNodeOperator.setData(path + "/status/" + ln + "/" + st, 
							("" + DateTime.now(DateTimeZone.UTC).minusSeconds(this.TTL).getMillis()).getBytes());
			}
			
			//default entitlements 
			if (zkov.exists(path + "/entitle/" + ln + "/" + cluster, false) == null)
				zkov.create(path + "/entitle/" + ln + "/" + cluster, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			if (zkov.exists(path + "/entitle/" + ln + "/" + cluster + "/" + ln, false) == null)
				zkov.create(path + "/entitle/" + ln + "/" + cluster + "/" + ln, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			
			for (CommandType ct : CommandType.values())
				if (zkov.exists(path + "/entitle/" + ln + "/" + cluster + "/" + ln + "/" + ct, false) == null)
					zkov.create(path + "/entitle/" + ln + "/" + cluster + "/" + ln + "/" + ct, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			
			if (zk.exists(path + "/indexes" + "/" + ln, false) == null)
				zk.create(path + "/indexes" + "/" + ln, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			
			if (zk.exists(path + "/events" + "/" + ln, false) == null)
				zk.create(path + "/events" + "/" + ln, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			
		} catch(KeeperException | InterruptedException e) {
			logger.error("Error setting data for node " + ln + " in cluster " + cluster, e);
		}
	}

	public void deleteNode(String cluster, int ln) throws TimeSeriesException {
		try {
			String path = TsrConstants.ROOT_PATH + "/" + cluster;
			deleteRecursiveOverlay(path + "/data/" + ln);
			deleteRecursiveOverlay(path + "/status/" + ln);
			deleteRecursiveOverlay(path + "/entitle/" + ln);
			deleteRecursiveOverlay(path + "/tsconfig/" + ln);
			deleteRecursive(path + "/indexes/" + ln);
			deleteRecursive(path + "/events/" + ln);
			deleteRecursive(path + "/patterns/" + ln);
			if (this.zk.exists(path + "/tasks", false) != null)
				for (String entry : this.zk.getChildren(path + "/tasks", false))
					if (entry.startsWith(ln + ";"))
						deleteRecursive(path + "/tasks/" + entry);

			if (this.zk.exists(path + "/assigned", false) != null)
				for (String entry : this.zk.getChildren(path + "/assigned", false))
					for (String wguid : this.zk.getChildren(path + "/assigned/" + entry, false))
						for (String taskPath : this.zk.getChildren(path + "/assigned/" + entry + "/" + wguid, false))
							if (taskPath.startsWith(ln + ";"))
								deleteRecursive(path + "/assigned/" + entry + "/" + taskPath);

		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Error deleting cluster = " + cluster + " and ln = " + ln, e);
		} catch (InterruptedException e) {
			logger.error("Error deleting cluster = " + cluster + " and ln = " + ln, e);
		}
	}
	
	public void setupCluster(String cluster) {
		CreateMode mode = CreateMode.PERSISTENT;
		String path = TsrConstants.ROOT_PATH + "/" + cluster;
		
		this.nodeOperator.createNode(TsrConstants.ROOT_PATH, new byte[0], mode);		//local 
		this.nodeOperator.createNode(path, new byte[0], mode);		//local 

		this.ovNodeOperator.createNode(TsrConstants.ROOT_PATH, new byte[0], mode);		//overlay 
		this.ovNodeOperator.createNode(path, new byte[0], mode);		//overlay
		
		this.ovNodeOperator.createNode(path + "/data", new byte[0], mode);		//overlay
		this.ovNodeOperator.createNode(path + "/status", new byte[0], mode);		//overlay
		this.nodeOperator.createNode(path + "/workers", new byte[0], mode);		//local 
		this.nodeOperator.createNode(path + "/assigned", new byte[0], mode);		//local 
		this.nodeOperator.createNode(path + "/assignedqueue", new byte[0], mode);		//local 
		this.ovNodeOperator.createNode(path + "/entitle", new byte[0], mode);		//overlay
		this.ovNodeOperator.createNode(path + "/tsconfig", new byte[0], mode);		//overlay
		this.ovNodeOperator.createNode(path + "/eventconfig", new byte[0], mode);		//overlay
		this.ovNodeOperator.createNode(path + "/eventconfig/queries", new byte[0], mode);		//overlay
		this.ovNodeOperator.createNode(path + "/eventconfig/streams", new byte[0], mode);		//overlay
		this.nodeOperator.createNode(path + "/tasks", new byte[0], mode);		//local 
		this.nodeOperator.createNode(path + "/taskqueue", new byte[0], mode);		//local 
		this.nodeOperator.createNode(path + "/syncs", new byte[0], mode);		//local 
		this.nodeOperator.createNode(path + "/syncqueue", new byte[0], mode);		//local 
		this.nodeOperator.createNode(path + "/syncworkers", new byte[0], mode);		//local 
		this.nodeOperator.createNode(path + "/eventsweepers", new byte[0], mode);		//local
		this.nodeOperator.createNode(path + "/eventworkers", new byte[0], mode);		//local
		this.nodeOperator.createNode(path + "/indexes", new byte[0], mode);		//local 
		this.nodeOperator.createNode(path + "/events", new byte[0], mode);		//local 
		this.ovNodeOperator.createNode(path + "/patterns", new byte[0], mode);		//overlay
	}
	
	public void deleteCluster(String cluster) throws TimeSeriesException {
			deleteRecursive(TsrConstants.ROOT_PATH + "/" + cluster);
			deleteRecursiveOverlay(TsrConstants.ROOT_PATH + "/" + cluster);
	}
	
}
