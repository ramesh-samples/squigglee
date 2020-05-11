package com.squigglee.coord.interfaces;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import com.squigglee.core.entity.SyncTask;

public interface ISyncService extends IConfigService {
	public void addSync(SyncTask task);
	public void logSync(SyncTask task, int ln);
	//public SortedMap<Integer,SyncTask> getSyncTasks(String cluster, int ln, int count); 
	public SortedMap<Integer,SyncTask> getSyncTasks(String cluster, int ln, List<Long> ids, int count, boolean isCEP);
	public Set<String> getCurrentClaims(String cluster, int nodeln);
}
