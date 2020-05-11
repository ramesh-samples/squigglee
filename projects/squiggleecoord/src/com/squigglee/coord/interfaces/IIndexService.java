package com.squigglee.coord.interfaces;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.TimeSeriesException;

public interface IIndexService  extends ICoordService {
	public byte[] loadSerializedIndex(String cluster, int ln, long id, String idxTableName) throws TimeSeriesException;
	public void saveSerializedIndex(String cluster, int ln, long id, String idxTableName, byte[] serialized, boolean create) throws TimeSeriesException;
	public void deleteSerializedIndex(String cluster, int ln, long id, String idxTableName);
	public int getNextMultiIndexId(String cluster, int ln, long id, String idxTableName);
	public void setNextMultiIndexId(String cluster, int ln, long id, String idxTableName, int nextId);
	public Map<String, List<IndexType>> getCurrentClaims(String cluster, int nodeln);
	public List<String> getCurrentClaims(String cluster, int nodeln, IndexType it);
}
