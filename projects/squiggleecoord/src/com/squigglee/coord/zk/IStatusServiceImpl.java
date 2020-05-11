package com.squigglee.coord.zk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.squigglee.coord.interfaces.IStatusService;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.StatusType;

public class IStatusServiceImpl extends ICoordServiceImpl implements IStatusService {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.IStatusServiceImpl");
	
	public List<NodeStatus> getClusterStatus(String cluster) {
		//"/" + cluster + "/" + ln + "/jobs/" + job
		SortedMap<Integer,NodeStatus> map = new TreeMap<Integer,NodeStatus>();
		NodeStatus ns = null;
		try {
			String pathString = TsrConstants.ROOT_PATH + "/" + cluster;
			List<String> nodeEntries = zkov.getChildren(pathString + "/data", false);
			for (String nodenum : nodeEntries) {
				ns = new NodeStatus();
				int ln = Integer.parseInt(nodenum);	// tasks
				ns.setLogicalNumber(ln);
				
				ns.setAddress(new String(zkov.getData(pathString + "/data/" + ln + "/addr", false, null)));
				ns.setDataCenter(new String(zkov.getData(pathString + "/data/" + ln + "/dc", false, null)));
				ns.setInstanceId(new String(zkov.getData(pathString + "/data/" + ln + "/iid", false, null)));
				ns.setName(new String(zkov.getData(pathString + "/data/" + ln + "/nm", false, null)));
				
				ns.setBootstrapNode( (new String(zkov.getData(pathString + "/data/" + ln + "/isboot", false, null))).equalsIgnoreCase("1")?true:false );
				ns.setSeedNode( (new String(zkov.getData(pathString + "/data/" + ln + "/isseed", false, null))).equalsIgnoreCase("1")?true:false );
				
				ns.setReplicaOf(Integer.parseInt(new String(zkov.getData(pathString + "/data/" + ln + "/replicaof", false, null))) );
				ns.setStorage( Integer.parseInt(new String(zkov.getData(pathString + "/data/" + ln + "/storage", false, null))) );
				ns.setStype(new String(zkov.getData(pathString + "/data/" + ln + "/stype", false, null)));
				
				DateTime storageStatus = new DateTime(Long.parseLong(new String(zkov.getData(TsrConstants.ROOT_PATH + "/" 
						+ cluster + "/status/" + ln + "/" + StatusType.NODE, false, null))));
				if (storageStatus.plusSeconds(TTL).isAfter(DateTime.now(DateTimeZone.UTC)))
					ns.setNodeUp(true);
				else
					ns.setNodeUp(false);
				
				DateTime viewStatus = new DateTime(Long.parseLong(new String(zkov.getData(TsrConstants.ROOT_PATH + "/" 
						+ cluster + "/status/" + ln + "/" + StatusType.OVERLAY, false, null))));
				if (viewStatus.plusSeconds(TTL).isAfter(DateTime.now(DateTimeZone.UTC)))
					ns.setOverlayUp(true);
				else
					ns.setOverlayUp(false);

				ns.setCluster(cluster);
				map.put(ln, ns);
			}
		} catch (KeeperException e) {
			logger.error("Error fetching cluster status for cluster = " + cluster, e);
		} catch (InterruptedException e) {
			logger.error("Error fetching cluster status for cluster = " + cluster, e);
		}
		return new ArrayList<NodeStatus>(map.values());
	}
	
	public Map<String,List<NodeStatus>> getGlobalStatus() {
		Map<String,List<NodeStatus>> map = new HashMap<String,List<NodeStatus>>();
		try {
			String pathString = TsrConstants.ROOT_PATH;
			
			List<String> nodeEntries = zkov.getChildren(pathString, false);
			for (String cluster : nodeEntries)
				map.put(cluster, getClusterStatus(cluster));
			
		} catch (KeeperException e) {
			logger.error("Error fetching global cluster status", e);
		} catch (InterruptedException e) {
			logger.error("Error fetching global cluster status", e);
		}
		return map;
	}
	
	public List<NodeStatus> getReplicaStatus(String cluster, int dataln) {
		//"/" + cluster + "/" + ln + "/jobs/" + job
		SortedMap<Integer,NodeStatus> map = new TreeMap<Integer,NodeStatus>();
		NodeStatus ns = null;
		try {
			String pathString = TsrConstants.ROOT_PATH + "/" + cluster;
			List<String> nodeEntries = zkov.getChildren(pathString + "/data", false);
			//logger.debug(nodeEntries);
			for (String nodenum : nodeEntries) {
				int nodelln = Integer.parseInt(nodenum);
				int nodeReplicaOf = Integer.parseInt(new String(zkov.getData(pathString + "/data/" + nodelln + "/replicaof", false, null)));
				if (nodeReplicaOf != dataln)
					continue;
				logger.debug("Found replicaof = " + nodeReplicaOf + " for data node = " + dataln);
				
				ns = new NodeStatus();
				ns.setLogicalNumber(nodelln);
				ns.setAddress(new String(zkov.getData(pathString + "/data/" + nodelln + "/addr", false, null)));
				ns.setDataCenter(new String(zkov.getData(pathString + "/data/" + nodelln + "/dc", false, null)));
				ns.setInstanceId(new String(zkov.getData(pathString + "/data/" + nodelln + "/iid", false, null)));
				ns.setName(new String(zkov.getData(pathString + "/data/" + nodelln + "/nm", false, null)));
				
				ns.setBootstrapNode( (new String(zkov.getData(pathString + "/data/" + nodelln + "/isboot", false, null))).equalsIgnoreCase("1")?true:false );
				ns.setSeedNode( (new String(zkov.getData(pathString + "/data/" + nodelln + "/isseed", false, null))).equalsIgnoreCase("1")?true:false );
				
				ns.setReplicaOf(nodeReplicaOf);
				ns.setStorage( Integer.parseInt(new String(zkov.getData(pathString + "/data/" + nodelln + "/storage", false, null))) );
				ns.setStype(new String(zkov.getData(pathString + "/data/" + nodelln + "/stype", false, null)));
				
				DateTime nodeStatus = new DateTime(Long.parseLong(new String(zkov.getData(TsrConstants.ROOT_PATH + "/" 
						+ cluster + "/status/" + nodelln + "/" + StatusType.NODE, false, null))));
				if (nodeStatus.plusSeconds(TTL).isAfter(DateTime.now(DateTimeZone.UTC)))
					ns.setNodeUp(true);
				else
					ns.setNodeUp(false);
				
				DateTime overlayStatus = new DateTime(Long.parseLong(new String(zkov.getData(TsrConstants.ROOT_PATH + "/" 
						+ cluster + "/status/" + nodelln + "/" + StatusType.OVERLAY, false, null))));
				if (overlayStatus.plusSeconds(TTL).isAfter(DateTime.now(DateTimeZone.UTC)))
					ns.setOverlayUp(true);
				else
					ns.setOverlayUp(false);

				ns.setCluster(cluster);
				logger.debug("Adding candidate = " + ns.getAddress());
				map.put(nodelln, ns);
			}
		} catch (KeeperException e) {
			logger.error("Error fetching cluster status for cluster = " + cluster, e);
		} catch (InterruptedException e) {
			logger.error("Error fetching cluster status for cluster = " + cluster, e);
		}
		return new ArrayList<NodeStatus>(map.values());
	}

	public NodeStatus getNodeStatus(String cluster, int nodeln) {
		NodeStatus ns = null;
		try {
			String pathString = TsrConstants.ROOT_PATH + "/" + cluster;
			ns = new NodeStatus();
			ns.setLogicalNumber(nodeln);
			ns.setAddress(new String(zkov.getData(pathString + "/data/" + nodeln + "/addr", false, null)));
			ns.setDataCenter(new String(zkov.getData(pathString + "/data/" + nodeln + "/dc", false, null)));
			ns.setInstanceId(new String(zkov.getData(pathString + "/data/" + nodeln + "/iid", false, null)));
			ns.setName(new String(zkov.getData(pathString + "/data/" + nodeln + "/nm", false, null)));
			
			ns.setBootstrapNode( (new String(zkov.getData(pathString + "/data/" + nodeln + "/isboot", false, null))).equalsIgnoreCase("1")?true:false );
			ns.setSeedNode( (new String(zkov.getData(pathString + "/data/" + nodeln + "/isseed", false, null))).equalsIgnoreCase("1")?true:false );
			
			ns.setReplicaOf(Integer.parseInt(new String(zkov.getData(pathString + "/data/" + nodeln + "/replicaof", false, null))));
			ns.setStorage( Integer.parseInt(new String(zkov.getData(pathString + "/data/" + nodeln + "/storage", false, null))) );
			ns.setStype(new String(zkov.getData(pathString + "/data/" + nodeln + "/stype", false, null)));
			
			DateTime nodeStatus = new DateTime(Long.parseLong(new String(zkov.getData(TsrConstants.ROOT_PATH + "/" 
					+ cluster + "/status/" + nodeln + "/" + StatusType.NODE, false, null))));
			if (nodeStatus.plusSeconds(TTL).isAfter(DateTime.now(DateTimeZone.UTC)))
				ns.setNodeUp(true);
			else
				ns.setNodeUp(false);
			
			DateTime overlayStatus = new DateTime(Long.parseLong(new String(zkov.getData(TsrConstants.ROOT_PATH + "/" 
					+ cluster + "/status/" + nodeln + "/" + StatusType.OVERLAY, false, null))));
			if (overlayStatus.plusSeconds(TTL).isAfter(DateTime.now(DateTimeZone.UTC)))
				ns.setOverlayUp(true);
			else
				ns.setOverlayUp(false);

			ns.setCluster(cluster);
		} catch (KeeperException e) {
			logger.error("Error fetching node status for cluster = " + cluster + " and ln = " + nodeln, e);
		} catch (InterruptedException e) {
			logger.error("Error fetching node status for cluster = " + cluster + " and ln = " + nodeln, e);
		}
		return ns;
	}
	
	public void setClusterStatus(String cluster, int ln, StatusType status) {
		byte[] statusData = ("" + DateTime.now(DateTimeZone.UTC).plusSeconds(this.TTL).getMillis()).getBytes();
		this.ovNodeOperator.setData(TsrConstants.ROOT_PATH + "/" + cluster + "/status/" + ln + "/" + status, statusData);	
	}

}
