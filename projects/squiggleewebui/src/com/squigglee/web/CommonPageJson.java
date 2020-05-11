// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

import java.util.ArrayList;
import java.util.List;

import com.squigglee.core.entity.NodeStatus;

public class CommonPageJson {

	private List<NodeStatus> clusterStatus = new ArrayList<NodeStatus>();
	private String isBoot;
	private WebAction action;
	private String message;
	private String error;
	
	public List<NodeStatus> getClusterStatus() {
		return clusterStatus;
	}
	public void setClusterStatus(List<NodeStatus> clusterStatus) {
		this.clusterStatus = clusterStatus;
	}
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
	public String getIsBoot() {
		return isBoot;
	}
	public void setIsBoot(String isBoot) {
		this.isBoot = isBoot;
	}
}
