package com.squigglee.coord.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.squigglee.coord.interfaces.IPatternService;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.TimeSeriesException;

public class IPatternServiceImpl extends ICoordServiceImpl implements IPatternService {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.IIndexServiceImpl");

	public boolean storePattern(String cluster, String pguid, List<String> pattern)
			throws TimeSeriesException {
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/patterns";
		this.ovNodeOperator.createNode(path, new byte[0], CreateMode.PERSISTENT);
		this.ovNodeOperator.createNode(path + "/" + pguid, new byte[0], CreateMode.PERSISTENT);
		for (int i=0; i < pattern.size(); i++)
			this.ovNodeOperator.createNode(path + "/" + pguid + "/" + i, pattern.get(i).toString().getBytes(), CreateMode.PERSISTENT);
		return true;
	}

	public List<String> fetchPattern(String cluster, String pguid)
			throws TimeSeriesException {
		List<String> ptrn = new ArrayList<String>();
		SortedMap<Integer,String> map = new TreeMap<Integer,String>();
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/patterns" + "/" + pguid;
			try {
				if (this.zkov.exists(path, false) != null)
					for (String entry : this.zkov.getChildren(path, false))
						map.put(Integer.parseInt(entry), new String(this.zkov.getData(path + "/" + entry, false, null)));
			} catch (KeeperException e) {
				logger.error("Error getting stored pattern data for path " + path, e);
			} catch (InterruptedException e) {
				logger.error("Error getting stored pattern data for path " + path, e);
			}
			ptrn.addAll(map.values());
		return ptrn;
	}

	public List<String> fetchCapturedPatterns(String cluster)
			throws TimeSeriesException {
		List<String> pguids = new ArrayList<String>();
		
		String path = TsrConstants.ROOT_PATH + "/" + cluster + "/patterns";
		try {
			if (this.zkov.exists(path, false) != null)
				for (String entry : this.zkov.getChildren(path, false))
					pguids.add(entry);
		} catch (KeeperException e) {
			logger.error("Error getting stored pattern data for path " + path, e);
		} catch (InterruptedException e) {
			logger.error("Error getting stored pattern data for path " + path, e);
		}
		
		return pguids;
	}

	
}
