// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeperMain;

import com.squigglee.coord.interfaces.ICoordService;
import com.squigglee.coord.interfaces.INode;
import com.squigglee.coord.utility.ZooKeeperFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;

public class ICoordServiceImpl implements ICoordService {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.ICoordServiceImpl");
	protected ZooKeeper zk = null;
	protected ZooKeeper zkov = null;
	protected INode nodeOperator = null;
	protected INode ovNodeOperator = null;
	protected int TTL = 20;	//seconds
	protected Random rand = null; 
	
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		
	}
	
	public void initialize(ZooKeeper zk, ZooKeeper zkov) throws TimeSeriesException {
		this.zk = zk;
		this.zkov = zkov;
		this.nodeOperator = ZooKeeperFactory.getNodeService(this.zk);
		this.ovNodeOperator = ZooKeeperFactory.getNodeService(this.zkov);
		this.TTL = LocalNodeProperties.getStatusTTL();
		this.rand = new Random();
	}

	public void close() throws TimeSeriesException {
		//try {
		//	if (zk != null)
		//		zk.close();
		//} catch (InterruptedException e) {
		//	logger.error("Failed to close ZooKeeper session properly", e);
		//	throw new TimeSeriesException("Failed to create coordination service, check log messages for details");
		//}
	}
	
	public void executeLine(String line) throws TimeSeriesException {
		executeLine(line, this.zk);
	}
	
	public void executeLineOv(String line) throws TimeSeriesException {
		executeLine(line, this.zkov);
	}
	
	private void executeLine(String line, ZooKeeper zooKeeper) throws TimeSeriesException {
		try {
			(new ZooKeeperMain(zooKeeper)).executeLine(line);
		} catch (InterruptedException e) {
			logger.error(e);
			throw new TimeSeriesException(e);
		} catch (IOException e) {
			logger.error(e);
			throw new TimeSeriesException(e);
		} catch (KeeperException e) {
			logger.error(e);
			throw new TimeSeriesException(e);
		}
	}
	
	public void deleteRecursive(String path) {
		deleteRecursive(path, this.zk, this.nodeOperator);
	}
	
	public void deleteRecursiveOverlay(String path) {
		deleteRecursive(path, this.zkov, this.ovNodeOperator);
	}
	
	private void deleteRecursive(String path, ZooKeeper zooKeeper, INode operator) {
		try {
			if ( zooKeeper.exists(path, false) != null ) {
				for (String entry : zooKeeper.getChildren(path, false)) 
					deleteRecursive(path + "/" + entry, zooKeeper, operator);
				
				//asynchronous
				//operator.deleteNode(path);
				
				//synchronous
				zooKeeper.delete(path, -1);
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Failed to recursively delete path " + path, e);
		} catch (InterruptedException e) {
			logger.error("Failed to recursively delete path " + path, e);
		}
	}
	
	public String getLocalIdString(long globalId) {
		int localid = (int) (globalId % TsrConstants.COLUMN_FAMILY_MAX_COLUMNS);
		String localidString = "" + localid;
		while (localidString.length() < 10)
			localidString = "0" + localidString;
		return localidString;
	}
	
	public int getLogicalNode(long globalId) {
		return TimeSeriesConfig.getLogicalNode(globalId);
	}
	
	public List<Integer> getReplicaSet(String cluster, long id) {
		return getReplicaSet(cluster, TimeSeriesConfig.getLogicalNode(id));
	}
	
	public List<Integer> getReplicaSet(String cluster, int dataln) {
		List<Integer> replicaSet = new ArrayList<Integer>();
		try {
			String path = TsrConstants.ROOT_PATH + "/" + cluster + "/data";
			for (String lnString : zkov.getChildren(path, false)) {
				int replicaof = Integer.parseInt(new String(zkov.getData(path + "/" + lnString + "/replicaof", false, null)));
				if (replicaof == dataln)
					replicaSet.add(Integer.parseInt(lnString));
			}
		} catch (KeeperException e) {
			logger.error("Found error getting replica set for cluster = " + cluster + " and ln = " + dataln, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting replica set for cluster = " + cluster + " and ln = " + dataln, e);
		}
		//System.out.println("Replica set for cluster " + cluster  + " and id = " + id + " = " + replicaSet);
		return replicaSet;
	}
}
