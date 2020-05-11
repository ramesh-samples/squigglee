// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.interfaces;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

public interface INode {
	void createNode(String path, byte[] data, CreateMode mode);
	void deleteNode(String path);
	void setData(String path, byte[] data);
	byte[] getNodeData(String path);
	INode initialize(ZooKeeper zk);
}
