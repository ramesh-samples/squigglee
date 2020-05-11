// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.interfaces;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesException;

public interface IStatusHandler extends IHandler {
	public void deleteNode(String cluster, int ln) throws TimeSeriesException;
	public void updateNodeStatus(String cluster, int ln) throws TimeSeriesException;
	public void updateOverlayStatus(String cluster, int ln) throws TimeSeriesException;
	public List<NodeStatus> fetchClusterStatus(String cluster) throws TimeSeriesException;
	public NodeStatus fetchNodeStatus(String cluster, int ln) throws TimeSeriesException;
	public Map<String,List<NodeStatus>> fetchGlobalStatus() throws TimeSeriesException;
}
