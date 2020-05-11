package com.squigglee.coord.interfaces;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.StatusType;

public interface IStatusService extends ICoordService {
	public NodeStatus getNodeStatus(String cluster, int nodeln);
	public List<NodeStatus> getClusterStatus(String cluster);
	public Map<String,List<NodeStatus>> getGlobalStatus();
	public List<NodeStatus> getReplicaStatus(String cluster, int replicaOf);
	public void setClusterStatus(String cluster, int ln, StatusType status);
}
