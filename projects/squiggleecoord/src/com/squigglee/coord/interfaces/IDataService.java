package com.squigglee.coord.interfaces;

import com.squigglee.core.entity.TimeSeriesException;

public interface IDataService extends ICoordService {
	public void setNode(String cluster, int ln, String addr, String dc, String iid, boolean isBoot, boolean isSeed, String nm, 
			int replicaOf, int storage, String stype) throws TimeSeriesException;
	public void deleteNode(String cluster, int ln) throws TimeSeriesException;
	public void setupCluster(String cluster);
	public void deleteCluster(String cluster) throws TimeSeriesException;
}
