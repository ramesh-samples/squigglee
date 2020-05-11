package com.squigglee.coord.zk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.squigglee.coord.interfaces.IEntitlementService;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.CommandType;

public class IEntitlementServiceImpl extends ICoordServiceImpl implements IEntitlementService {
	
	public Map<String, Map<Integer, List<CommandType>>> getEntitlements(String cluster, int ln) {
		
		Map<String, Map<Integer, List<CommandType>>> entitlements = new HashMap<String, Map<Integer, List<CommandType>>>(); 
		String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/entitle/" + ln;
		try {
			for (String objCluster : zkov.getChildren(pathString, false)) {
				if (!entitlements.containsKey(objCluster))
					entitlements.put(objCluster, new HashMap<Integer,List<CommandType>>());
				for (String objLnString : zkov.getChildren(pathString + "/"+  objCluster, false)) {
					Integer objLn = Integer.parseInt(objLnString);
					if (!entitlements.get(objCluster).containsKey(objLn))
						entitlements.get(objCluster).put(objLn, new ArrayList<CommandType>());
					for (String commandTypeString : zkov.getChildren(pathString + "/"+  objCluster + "/" + objLn, false)) {
						entitlements.get(objCluster).get(objLn).add(CommandType.valueOf(commandTypeString));
					}
				}
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return entitlements;
	}
	
	public void setEntitlement(String clusterSubj, int lnSubj,	String clusterObj, int lnObj, List<CommandType> permissions) {
		CreateMode mode = CreateMode.PERSISTENT;
		this.ovNodeOperator.createNode(TsrConstants.ROOT_PATH + "/" + clusterSubj + "/entitle/" + lnSubj + "/" + clusterObj, new byte[0], mode);
		this.ovNodeOperator.createNode(TsrConstants.ROOT_PATH + "/" + clusterSubj + "/entitle/" + lnSubj + "/" + clusterObj + "/" + lnObj, new byte[0], mode);
		for (CommandType ct : permissions)
			this.ovNodeOperator.createNode(TsrConstants.ROOT_PATH + "/" + clusterSubj + "/entitle/" + lnSubj + "/" + clusterObj + "/" + lnObj + "/" + ct.toString(), new byte[0], mode);
	}
	
	public void deleteEntitlement(String clusterSubj, int lnSubj, String clusterObj, int lnObj) {
		deleteRecursiveOverlay(TsrConstants.ROOT_PATH + "/" + clusterSubj + "/" + lnSubj + "/entitle/" + clusterObj + "/" + lnObj);
	}	

}
