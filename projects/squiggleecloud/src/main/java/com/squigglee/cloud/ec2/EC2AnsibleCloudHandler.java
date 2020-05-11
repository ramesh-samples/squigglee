package com.squigglee.cloud.ec2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.squigglee.cloud.interfaces.ICloudHandler;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;

public class EC2AnsibleCloudHandler implements ICloudHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.cloud.ec2.EC2AnsibleCloudHandler");
	ScriptLauncher launcher = null;
	EC2Mapper mapper = new EC2Mapper();
	
	public EC2AnsibleCloudHandler() {
		launcher = new ScriptLauncher();
	}

	@Override
	public int createTopology(List<Map<String, String>> topology) throws TimeSeriesException {
		logger.info("Received topology create request for topology = " + topology + " and cluster = " + LocalNodeProperties.getClusterName());
		StringBuilder builder = new StringBuilder("topology=[");
		//int dataNode = findDataNodeInTopology(topology);
		int count = 0;
		for (Map<String,String> node : topology) {
			builder.append(count++==0?"{":",{");
			builder.append("\"cluster\":\"" + node.get("cluster") + "\",");
			builder.append("\"instance_type\":\"" + mapper.getType(node.get("stype")) + "\",");
			builder.append("\"image\":\"" + mapper.getImage(node.get("dc")) + "\",");
			builder.append("\"group\":\"" + mapper.getGroup(node.get("dc")) + "\",");
			builder.append("\"region\":\"" + node.get("dc") + "\",");
			builder.append("\"zone\":\"" + mapper.getZone(node.get("dc")) + "\",");
			builder.append("\"vpc_subnet_id\":\"" + mapper.getSubnet(node.get("dc")) + "\",");
			builder.append("\"token\":\"" + Long.parseLong(node.get("ln"))*1000000000 + "\",");
			builder.append("\"ring_position\":\"" + node.get("ln") + "\",");
			builder.append("\"is_seed\":\"" + (Boolean.parseBoolean(node.get("isSeed"))?"yes":"no") + "\",");
			builder.append("\"is_bootstrap_node\":\"" + (Boolean.parseBoolean(node.get("isBoot"))?"yes":"no") + "\",");
			builder.append("\"Name\":\"" + "TimeSeriesNode_" + node.get("ln") + "\",");
			builder.append("\"replica_of\":\"" + node.get("replicaOf") + "\",");
			builder.append("\"storage\":\"" + node.get("storage") + "\",");
			builder.append("\"stype\":\"" + node.get("stype") + "\"");
			builder.append("}");
		}
		builder.append("]");
		String request = builder.toString();
		logger.info("Create parameters = " + request);
		int result = launcher.Launch(ScriptType.CREATE_TOPOLOGY, request);
		System.out.println("Result of create topology request = " + result);
		logger.info("Result of create topology request = " + result);
		if (result != 0)
			throw new TimeSeriesException("Failed to create new node");
		return result;
	}

	@Override
	public int deleteTopology(List<Map<String, String>> topology) throws TimeSeriesException {
		logger.info("Received topology delete request for topology = " + topology + " and cluster = " + LocalNodeProperties.getClusterName());
		StringBuilder builder = new StringBuilder("topology=[");
		int count = 0;
		for (Map<String,String> node : topology) {
			builder.append(count++==0?"{":",{");
			builder.append("\"ec2_id\":\"" + node.get("iid") + "\",");
			builder.append("\"cluster\":\"" + node.get("cluster") + "\",");
			builder.append("\"region\":\"" + node.get("dc") + "\",");
			builder.append("\"image\":\"" + mapper.getImage(node.get("dc")) + "\",");
			builder.append("\"ec2_ip_address\":\"" + node.get("addr") + "\"");
			builder.append("}");
		}
		builder.append("]");
		String request = builder.toString();
		
		logger.info("Delete parameters = " + request);
		int result = launcher.Launch(ScriptType.DELETE_TOPOLOGY, request);
		System.out.println("Result of delete topology request = " + result);
		logger.info("Result of delete topology request = " + result);
		if (result != 0)
			throw new TimeSeriesException("Failed to delete node");
		return result;
	}
	
	@Override
	public int restartCoordinationService(String cluster, int logicalNumber, int replicaOf) {
		int count = 0;
		int maxCount = 10;  
		int result = 3;
		logger.info("Received configure node request for node = " + logicalNumber + " replica of " + replicaOf + " in cluster = " + cluster);
		while (result != 0) {
			System.out.println("Configuration attempt number = " + ++count);
			logger.debug("Configuration attempt number = " + ++count);
			try {
				result = launcher.Launch(ScriptType.START_SERVICES_COORD, "logicalNumber=" + logicalNumber + " cluster='" + cluster + "' replicaOf=" + replicaOf + "");
			} catch (Exception ex) {
				logger.error("Error launching script for restarting coordination services in cluster " + cluster,ex);
			}
			if (count == maxCount)			 
				break;
		}
		return result;
	}

	@Override
	public int configureNode(String cluster, int logicalNumber, int replicaOf) {
		int count = 0;
		int maxCount = 10;  
		int result = 3;
		logger.info("Received configure node request for node = " + logicalNumber + " replica of " + replicaOf + " in cluster = " + cluster);
		while (result != 0) {
			System.out.println("Configuration attempt number = " + ++count);
			logger.debug("Configuration attempt number = " + ++count);
			try {
				result = launcher.Launch(ScriptType.CONFIGURE, "logicalNumber=" + logicalNumber + " cluster='" + cluster + "' replicaOf=" + replicaOf + "");
			} catch (Exception ex) {
				logger.error("Error launching script for reconfiguring node " + logicalNumber + " in cluster " + cluster,ex);
			}
			//RKR TODO commenting the following as a pause is not needed between configurations TODO verify this
			//try {
			//	Thread.sleep(60000);
			//} catch (InterruptedException e) {
			//	logger.error("Failed to configure node " + logicalNumber,e);
			//}
			if (count == maxCount)			 
				break;
		}
		return result;
	}

	@Override
	public int startAllNodes() throws TimeSeriesException {
		logger.info("Received start node request for all nodes" + " and cluster = " + LocalNodeProperties.getClusterName());
		return launcher.Launch(ScriptType.START_ALL, null);
	}

	@Override
	public int startNode(String cluster, int logicalNumber) {
		logger.info("Received start node request for node = " + logicalNumber + " and cluster = " + cluster);
		int result = launcher.Launch(ScriptType.START_NODE, "logicalNumber=" + logicalNumber + " cluster='" + cluster + "'");
		return result;
	}

	@Override
	public int setSchema() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int startServicesAllNodes() {
		logger.info("Received start services request for all nodes");
		return launcher.Launch(ScriptType.START_SERVICES_ALL, null);
	}

	@Override
	public int startServicesAtNode(String cluster, int logicalNumber) {
		logger.info("Received start services request for node = " + logicalNumber + " and cluster = " + cluster);
		int result = launcher.Launch(ScriptType.START_SERVICES_NODE, "logicalNumber=" + logicalNumber + " cluster='" + cluster + "'");
		return result;
	}
	
	@Override
	public int stopServicesAtNode(String cluster, int logicalNumber) {
		logger.info("Received stop services request for node = " + logicalNumber + " and cluster = " + cluster);
		int result = launcher.Launch(ScriptType.STOP_SERVICES_NODE, "logicalNumber=" + logicalNumber + " cluster='" + cluster + "'");
		return result;
	}

	@Override
	public List<String> getDataCenters() {
		return mapper.getRegions();
	}

	@Override
	public List<String> getServerTypes() {
		return Arrays.asList(new String[]{"Small","Medium","Large"});
	}

	@Override
	public int findDataNodeInTopology(List<Map<String,String>> topology) throws TimeSeriesException {
		int dataNode = -1;
		for (Map<String,String> node : topology) {
			if(!Boolean.parseBoolean(node.get("isReplica"))) {
				if (dataNode >=0)
					throw new TimeSeriesException("Multiple data nodes cannot be specified in a single topology");
				dataNode = Integer.parseInt(node.get("ln"));
					
			}
		}
		if (dataNode < 0)
			throw new TimeSeriesException("A single data node must be listed in each requested topology");
		return dataNode;
	}
	
}
