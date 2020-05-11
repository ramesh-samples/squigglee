package com.squigglee.cloud.interfaces;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.TimeSeriesException;

public interface ICloudHandler {
	
	public int createTopology(List<Map<String,String>> topology) throws TimeSeriesException;
	public int deleteTopology(List<Map<String, String>> topology) throws TimeSeriesException;
	public int configureNode(String cluster, int logicalNumber, int replicaOf);
	public int startAllNodes() throws TimeSeriesException;
	public int startNode(String cluster, int logicalNumber) ;
	public int setSchema();
	public int startServicesAllNodes();
	public int startServicesAtNode(String cluster, int logicalNumber);
	public int restartCoordinationService(String cluster, int logicalNumber, int replicaOf);
	public List<String> getDataCenters();
	public List<String> getServerTypes();
	public int findDataNodeInTopology(List<Map<String,String>> topology) throws TimeSeriesException ;
	int stopServicesAtNode(String cluster, int logicalNumber) throws TimeSeriesException;
}
