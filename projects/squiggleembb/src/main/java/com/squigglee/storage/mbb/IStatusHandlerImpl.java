// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mbb;

import java.util.List;
import java.util.Map;

import com.squigglee.coord.storage.StatusHandlerMixin;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IStatusHandler;

public class IStatusHandlerImpl extends ISchemaHandlerImpl implements IStatusHandler {
	protected StatusHandlerMixin shMixin = null;
	protected static int TTL = 60; // Status time to live in seconds 
	
	@Override
	public void updateNodeStatus(String cluster, int ln) throws TimeSeriesException {
		shMixin.updateNodeStatus(cluster, ln);
	}

	@Override
	public void updateOverlayStatus(String cluster, int ln) throws TimeSeriesException {
		shMixin.updateOverlayStatus(cluster, ln);
	}

	@Override
	public List<NodeStatus> fetchClusterStatus(String cluster) throws TimeSeriesException {
		return shMixin.fetchClusterStatus(cluster);
	}
	
	@Override
	public NodeStatus fetchNodeStatus(String cluster, int ln) throws TimeSeriesException {
		return shMixin.fetchNodeStatus(cluster, ln);
	}
	
	@Override
	public Map<String,List<NodeStatus>> fetchGlobalStatus() throws TimeSeriesException {
		return shMixin.fetchGlobalStatus();
	}
	
	@Override
	public void initialize() {
		super.initialize();
		this.shMixin = new StatusHandlerMixin();
	}

	@Override
	public void deleteNode(String cluster, int ln) throws TimeSeriesException {
		shMixin.deleteNode(cluster, ln);
	}

}
