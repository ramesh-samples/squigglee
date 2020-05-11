// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.utility;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.squigglee.coord.interfaces.INode;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;

public class ZooKeeperFactory {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.AsyncFactory");
	protected static final String authpw = "super:zkadminpw"; //TODO move to config file
	
	public static INode getNodeService(ZooKeeper zk) {
		
		return new INode() {
			private ZooKeeper zk = null;
			public void createNode(String path, byte[] data, CreateMode mode) {
				zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode, getNodeCreatorCallback(this, mode), data);
			}

			public void deleteNode(String path) {
				zk.delete(path, -1, getNodeDeletorCallback(this), path);
			}
			
			public byte[] getNodeData(String path) {
				try {
					return zk.getData(path, false, null);
				} catch (KeeperException e) {
					logger.error("Found error getting node data for path = " + path, e);
				} catch (InterruptedException e) {
					logger.error("Found error getting node data for path = " + path, e);
				}
				return null;
			}
			
			public INode initialize(ZooKeeper zk) {
				this.zk = zk;
				return this;
			}

			public void setData(String path, byte[] data) {
				zk.setData(path, data, -1, setNodeDataCallback(this), data);
			}
		}.initialize(zk);
	}

	public static ZooKeeper getLocalZooKeeper() throws TimeSeriesException {
		ZooKeeper zk = null;
		try {
			String localzkaddr = "zk" + LocalNodeProperties.getNodeLogicalNumber() + "." + LocalNodeProperties.getClusterName() + ".squigglee:2181";
			zk = new ZooKeeper(localzkaddr, LocalNodeProperties.getSessionTimeoutCoord(), 
							new Watcher() {
					private Logger logger = Logger.getLogger("com.squigglee.coord.zk.ZooKeeperFactory");
					public void process(WatchedEvent event) {
						logger.debug("EventType = " + event.getType() + ", KeeperState = " + event.getState() + " , path = " + event.getPath());
					}
				});
			zk.addAuthInfo("digest", authpw.getBytes());
		} catch (Exception e) {
			logger.error("Failed to create local zookeeper for " + LocalNodeProperties.getNodeLogicalNumber() + " and cluster seeds = " + LocalNodeProperties.getClusterSeedsCoord());
		}
		return zk;
	}
		
	public static ZooKeeper getLocalOverlayZooKeeper() throws TimeSeriesException {
		ZooKeeper zk = null;
		try {
			String localzkaddr = "zk" + LocalNodeProperties.getNodeLogicalNumber() + "." + LocalNodeProperties.getClusterName() + ".squigglee:2191";
			zk = new ZooKeeper(localzkaddr, LocalNodeProperties.getSessionTimeoutCoord(), 
					new Watcher() {
				private Logger logger = Logger.getLogger("com.squigglee.coord.zk.ZooKeeperFactory");
				public void process(WatchedEvent event) {
					logger.debug("EventType = " + event.getType() + ", KeeperState = " + event.getState() + " , path = " + event.getPath());
				}
			});
			zk.addAuthInfo("digest", authpw.getBytes());
		} catch (Exception e) {
			logger.error("Failed to create local overlay zookeeper for " + LocalNodeProperties.getNodeLogicalNumber() + " and cluster seeds = " 
					+ LocalNodeProperties.getClusterSeedsCoord());
		}
		return zk;
	}
	
	private static StringCallback getNodeCreatorCallback(final INode nodeOperation, CreateMode mode) {
		return new StringCallback() {
			private CreateMode mode = CreateMode.PERSISTENT;
			public void processResult(int rc, String path, Object ctx, String name) {
				Code c = Code.get(rc);
				switch(c) {
				case CONNECTIONLOSS:
					logger.error("Found connection loss error creating path " + path);
					nodeOperation.createNode(path, (byte[]) ctx, this.mode); 
					break;
				case OK:
					break;
				case NODEEXISTS:
					//logger.info("Found node exists exception for path = " + path);
					nodeOperation.setData(path, (byte[]) ctx);
					break;
				default:
					logger.error("Found error " + Code.get(rc) + " while creating path " + path);
					break;
				}
			}
			public StringCallback initialize(CreateMode mode) {
				this.mode = mode;
				return this;
			}
		}.initialize(mode);
	}

	private static VoidCallback getNodeDeletorCallback(final INode nodeOperation) {
		return new VoidCallback() {
			public void processResult(int rc, String path, Object ctx) {
				Code c = Code.get(rc);
				switch(c) {
				case CONNECTIONLOSS:
					logger.error("Found connection loss error creating path " + path);
					nodeOperation.deleteNode(path);
					break;
				case OK:
					break;
				case NONODE:
					//logger.info("No node exists for delete request for path = " + path);
					break;
				case NOTEMPTY:
					//logger.info("Node has children for delete request for path = " + path);
					break;
				default:
					logger.error("Found error " + Code.get(rc) + " while creating path " + path);
					break;
				}
			}
		};
	}
		
	private static StatCallback setNodeDataCallback(final INode nodeOperation) {
		return new StatCallback() {
			public void processResult(int rc, String path, Object ctx, Stat stat) {
				Code c = Code.get(rc);
				switch(c) {
				case CONNECTIONLOSS:
					logger.error("Found connection loss error creating path " + path);
					nodeOperation.setData(path, (byte[]) ctx);
					break;
				case OK:
					//if (path.contains("indexes"))
					//	System.out.println("Index save successful " + path);
					break;
				case NONODE:
					break;
				default:
					System.out.println("Found error " + Code.get(rc) + " while creating path " + path);
					break;
				}
			}
		};
	}

}
