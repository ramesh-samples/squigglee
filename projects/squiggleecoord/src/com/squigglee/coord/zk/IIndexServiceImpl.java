package com.squigglee.coord.zk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.squigglee.coord.interfaces.IIndexService;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.TimeSeriesException;

public class IIndexServiceImpl extends ICoordServiceImpl implements IIndexService {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.IIndexServiceImpl");

	public byte[] loadSerializedIndex(String cluster, int ln, long id, String idxTableName) throws TimeSeriesException {

		String indexTableName = idxTableName + "_" + id;
		byte[] serialized = null;
		//byte[] srlIndex = null;
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/indexes" + "/" + ln + "/" + indexTableName;
		try {
			if (this.zk.exists(path, false) != null)
				serialized = this.zk.getData(path, false, null);
			if (serialized == null)
				return null;
			//srlIndex = javax.xml.bind.DatatypeConverter.parseBase64Binary(new String(serialized));
			System.out.println("Size (kb) of retrieved serialized index " + indexTableName + " = " + serialized.length*1.0/1024.0);
			logger.debug("Size (kb) of retrieved serialized index " + indexTableName + " = " + serialized.length*1.0/1024.0);
		} catch (KeeperException e) {
			if (e.code().equals(KeeperException.Code.NONODE))
				logger.info("Serialized index does not exist at path = " + path + ", will create new", e);
			else
				logger.error("Error loading serialized index for path = " + path, e);
		} catch (InterruptedException e) {
			logger.error("Error loading serialized index for path = " + path, e);
		}
		
		return serialized;
	}

	public void saveSerializedIndex(String cluster, int ln, long id, String idxTableName, byte[] serialized, boolean create) throws TimeSeriesException {
		String indexTableName = idxTableName + "_" + id;
		if (serialized == null)
			return;
		//String contents = javax.xml.bind.DatatypeConverter.printBase64Binary(serialized);
		if (create)
			this.nodeOperator.createNode(TsrConstants.ROOT_PATH + "/" + cluster + "/indexes" + "/" + ln + "/" 
				+ indexTableName, serialized, CreateMode.PERSISTENT);
		else
			this.nodeOperator.setData(TsrConstants.ROOT_PATH + "/" + cluster + "/indexes" + "/" + ln + "/" 
				+ indexTableName, serialized);
		
		System.out.println("Size (kb) of stored serialized index " + indexTableName + " = " + serialized.length*1.0/1024.0);
		logger.debug("Size (kb) of stored serialized index " + indexTableName + " = " + serialized.length*1.0/1024.0);
	}
	
	public void deleteSerializedIndex(String cluster, int ln, long id, String idxTableName) {
		String indexTableName = idxTableName + "_" + id;
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/indexes" + "/" + ln + "/" + indexTableName;
		this.nodeOperator.deleteNode(path);
	}

	public int getNextMultiIndexId(String cluster, int ln, long id, String idxTableName) {
		int currentId = 0;
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/indexes" + "/" + ln + "/" + idxTableName + "_" + id + "/nextId";
		try {
			if (zk.exists(path, false) == null)
				this.nodeOperator.createNode(path, ("" + currentId).getBytes(), CreateMode.PERSISTENT);
			else {
				currentId = Integer.parseInt(new String(zk.getData(path, false, null)));
			}
		} catch (KeeperException e) {
			logger.error("Found error setting next data for index = " + idxTableName + " for id = " + id + " for ln = " + ln + " for cluster = " + cluster, e);
		} catch (InterruptedException e) {
			logger.error("Found error setting next data for index = " + idxTableName + " for id = " + id + " for ln = " + ln + " for cluster = " + cluster, e);
		}

		return currentId;
	}
	
	public void setNextMultiIndexId(String cluster, int ln, long id, String idxTableName, int nextId) {
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/indexes" + "/" + ln + "/" + idxTableName + "_" + id + "/nextId";
		this.nodeOperator.setData(path, ("" + (nextId + 1)).getBytes());
	}
	
	public Map<String, List<IndexType>> getCurrentClaims(String cluster, int nodeln) {
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/workers";
		Map<String, List<IndexType>> claimList = new HashMap<String, List<IndexType>>();
		try {
			for (String entry : this.zk.getChildren(path, false)) {
				String[] tokens = entry.split(";");		//ln; index type; guid
				if (Integer.parseInt(tokens[0]) == nodeln) {
					IndexType it = IndexType.valueOf(tokens[1]);
					if (!claimList.containsKey(tokens[2]))
						claimList.put(tokens[2], new ArrayList<IndexType>());
					if (!claimList.get(tokens[2]).contains(it))
						claimList.get(tokens[2]).add(it);
				}
			}
		} catch (KeeperException e) {
			logger.error("Error getting current claims for node " + nodeln + " in cluster " + cluster, e);
		} catch (InterruptedException e) { 
			logger.error("Error getting current claims for node " + nodeln + " in cluster " + cluster, e);
		}
		return claimList;
	}
	
	public List<String> getCurrentClaims(String cluster, int nodeln, IndexType it) {
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/workers";
		List<String> claimList = new ArrayList<String>();
		try {
			for (String entry : this.zk.getChildren(path, false)) {
				String[] tokens = entry.split(";");		//ln; index type; guid
				if (Integer.parseInt(tokens[0]) == nodeln && it.equals(IndexType.valueOf(tokens[1])))
					if (!claimList.contains(tokens[2]))
						claimList.add(tokens[2]);
			}
		} catch (KeeperException e) {
			logger.error("Error getting current claims for node " + nodeln + " in cluster " + cluster, e);
		} catch (InterruptedException e) { 
			logger.error("Error getting current claims for node " + nodeln + " in cluster " + cluster, e);
		}
		return claimList;
	}
}
