// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

import java.util.ArrayList;
import java.util.List;

import com.squigglee.core.entity.NodeStatus;

public class OperatePageJson {

	private WebAction action;
	private String message;
	private String error;
	private String cluster;
	
	private List<NodeStatus> clusterStatus = new ArrayList<NodeStatus>();
	
	public WebAction getAction() {
		return action;
	}
	public void setAction(WebAction action) {
		this.action = action;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}
	public List<NodeStatus> getClusterStatus() {
		return clusterStatus;
	}
	public void setClusterStatus(List<NodeStatus> clusterStatus) {
		this.clusterStatus = clusterStatus;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
}
