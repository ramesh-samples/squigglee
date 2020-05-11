// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.entity;

public class NodeStatus {
	private int logicalNumber;
	private String address;
	private String dataCenter;
	private String instanceId;
	private String name;
	private boolean isBootstrapNode;
	private boolean isSeedNode;
	private int replicaOf;
	private int storage;
	private String stype;
	private String cluster;
	private boolean isNodeUp;
	private boolean isOverlayUp;
	private String errorMessage;
		
	public int getLogicalNumber() {
		return logicalNumber;
	}
	public void setLogicalNumber(int logicalNumber) {
		this.logicalNumber = logicalNumber;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getDataCenter() {
		return dataCenter;
	}
	public void setDataCenter(String dataCenter) {
		this.dataCenter = dataCenter;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public boolean isBootstrapNode() {
		return isBootstrapNode;
	}
	public void setBootstrapNode(boolean isBootstrapNode) {
		this.isBootstrapNode = isBootstrapNode;
	}
	public boolean isSeedNode() {
		return isSeedNode;
	}
	public void setSeedNode(boolean isSeedNode) {
		this.isSeedNode = isSeedNode;
	}
	public int getReplicaOf() {
		return replicaOf;
	}
	public void setReplicaOf(int replicaOf) {
		this.replicaOf = replicaOf;
	}
	public int getStorage() {
		return storage;
	}
	public void setStorage(int storage) {
		this.storage = storage;
	}
	public String getStype() {
		return stype;
	}
	public void setStype(String stype) {
		this.stype = stype;
	}
	public String getInstanceId() {
		return instanceId;
	}
	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	public boolean isOverlayUp() {
		return isOverlayUp;
	}
	public void setOverlayUp(boolean isOverlayUp) {
		this.isOverlayUp = isOverlayUp;
	}
	public boolean isNodeUp() {
		return isNodeUp;
	}
	public void setNodeUp(boolean isNodeUp) {
		this.isNodeUp = isNodeUp;
	}
	public String getErrorMessage() {
		return errorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

}
