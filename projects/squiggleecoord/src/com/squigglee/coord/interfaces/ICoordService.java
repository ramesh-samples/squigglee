// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.interfaces;

import java.util.List;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.squigglee.core.entity.TimeSeriesException;

public interface ICoordService extends Watcher {
	public void initialize(ZooKeeper zk, ZooKeeper zkov) throws TimeSeriesException;	
	public void close() throws TimeSeriesException;
	public void executeLine(String line) throws TimeSeriesException ;
	public void executeLineOv(String line) throws TimeSeriesException ;
	public String getLocalIdString(long globalId);
	public int getLogicalNode(long globalId);
	public List<Integer> getReplicaSet(String cluster, long id);
	public List<Integer> getReplicaSet(String cluster, int dataln);
	public void deleteRecursive(String path);
	public void deleteRecursiveOverlay(String path);
}
