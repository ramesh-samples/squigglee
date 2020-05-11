package com.squigglee.coord.interfaces;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.IndexingTask;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesConfig;

public interface IConfigService extends ICoordService {
	
	public MasterData getConfig(IndexingTask task);
	public Map<String,List<Long>> getConfig(String cluster, int ln, String guid);
	public Map<String,List<TimeSeriesConfig>> getConfig(String cluster, int ln);
	public Map<String, Map<Integer, Map<String,List<TimeSeriesConfig>>>> getConfig();
	public Map<Integer, Map<String,List<TimeSeriesConfig>>> getConfig(String cluster);
	
	public boolean isConfigured(String cluster, int ln, String guid);
	public List<Long> createConfig(TimeSeriesConfig config);
	public List<Long> deleteConfig(TimeSeriesConfig config);
	
	public Map<String,Map<String, Map<Integer, List<String>>>> getOverlayNetwork();
	
	public Map<Long,Map<IndexType,List<String>>> getIndexList(String cluster, int ln);	
	public Map<String,Map<IndexType,List<String>>> getGuidIndexList(String cluster, int ln);
	public List<String> getGuidIndexList(String cluster, int ln, IndexType it);
	
	public MasterData getMasterData(String cluster, long id);
	public List<Long> getMasterDataIds(String cluster, int ln, List<String> guids);
	
	public int createMasterData(List<MasterData> list);
	public int updateMasterData(List<MasterData> list);
	
	public void updateDataMinMax(MasterData md, long min, long max);
	public long[] getDataMinMax(MasterData md);
	
	public boolean isConfiguredWithIndex(String cluster, long id, String index);
	
	public List<MasterData> getMasterData(String cluster, int ln, String guid);
	public List<MasterData> getConfiguredMasterData(String cluster, int ln, String guid, long startts, long endts);
}
