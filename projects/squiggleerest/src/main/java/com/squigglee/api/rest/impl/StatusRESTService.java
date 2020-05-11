package com.squigglee.api.rest.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.squigglee.api.rest.IStatusRESTService;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;

public class StatusRESTService extends RestBase implements IStatusRESTService {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.StatusRESTService");

	@Override
	public Map<String, List<NodeStatus>> fetchGlobalStatus() {
		try {
			logger.debug("Received request to fetch the global status across all clusters");
			System.out.println("Received request to fetch the global status across all clusters");
			return HandlerFactory.getStatusHandler().fetchGlobalStatus();
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching global status ", e);
			Map<String, List<NodeStatus>> map = new HashMap<String, List<NodeStatus>>();
			map.put("", new ArrayList<NodeStatus>());
			NodeStatus errorStatus = new NodeStatus();
			errorStatus.setErrorMessage(e.getMessage());
			map.get("").add(errorStatus);
			return map;
		}
	}

	@Override
	public List<NodeStatus> fetchClusterStatus(String cluster) {
		try {
			logger.debug("Received request to fetch the node status for cluster " + cluster);
			System.out.println("Received request to fetch the node status for cluster " + cluster);
			return HandlerFactory.getStatusHandler().fetchClusterStatus(cluster);
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching global status ", e);
			List<NodeStatus> list = new ArrayList<NodeStatus>();
			NodeStatus errorStatus = new NodeStatus();
			errorStatus.setCluster(cluster);
			errorStatus.setErrorMessage(e.getMessage());
			list.add(errorStatus);
			return list;
		}
	}
	
	@Override
	public NodeStatus fetchNodeStatus(String cluster, int ln) {
		try {
			logger.debug("Received request to fetch the node status for node " + ln + " in cluster " + cluster);
			System.out.println("Received request to fetch the node status for node " + ln + " in cluster " + cluster);
			return HandlerFactory.getStatusHandler().fetchNodeStatus(cluster, ln);
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching global status ", e);
			NodeStatus errorStatus = new NodeStatus();
			errorStatus.setCluster(cluster);
			errorStatus.setLogicalNumber(ln);
			errorStatus.setErrorMessage(e.getMessage());
			return errorStatus;
		}
	}

	@Override
	public Boolean deleteNode(String cluster, int ln) {
		boolean result = false;
		NodeStatus ns = new NodeStatus();
		ns.setCluster(cluster);
		ns.setLogicalNumber(ln);
		try {
			HandlerFactory.getStatusHandler().deleteNode(cluster, ln);
			result = true;
			logger.debug("Deleted node " + ln + " in cluster " + cluster);
			System.out.println("Deleted node " + ln + " in cluster " + cluster);
		} catch (TimeSeriesException e) {
			logger.error("Found error deleting node for cluster = " + cluster + " and ln = " + ln, e);
			ns.setErrorMessage(e.getMessage());
		}
		return result;
	}

	@Override
	public NodeStatus updateNodeStatus(String cluster, int ln) {
		NodeStatus ns = new NodeStatus();
		ns.setCluster(cluster);
		ns.setLogicalNumber(ln);
		try {
			HandlerFactory.getStatusHandler().updateNodeStatus(cluster, ln);
			return HandlerFactory.getStatusHandler().fetchNodeStatus(cluster, ln);
		} catch (TimeSeriesException e) {
			logger.error("Found error deleting node for cluster = " + cluster + " and ln = " + ln, e);
			ns.setErrorMessage(e.getMessage());
		}
		return ns;
	}
	
	@Override
	public NodeStatus updateOverlayStatus(String cluster, int ln) {
		NodeStatus ns = new NodeStatus();
		ns.setCluster(cluster);
		ns.setLogicalNumber(ln);
		try {
			HandlerFactory.getStatusHandler().updateOverlayStatus(cluster, ln);
			return HandlerFactory.getStatusHandler().fetchNodeStatus(cluster, ln);
		} catch (TimeSeriesException e) {
			logger.error("Found error deleting node for cluster = " + cluster + " and ln = " + ln, e);
			ns.setErrorMessage(e.getMessage());
		}
		return ns;
	}
}
