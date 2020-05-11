package com.squigglee.coord.interfaces;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.squigglee.core.entity.IndexingTask;

public interface ITaskService extends IConfigService {
	
	public void addTask(IndexingTask request);
	public void deleteTask(IndexingTask request);
	public void deleteAssignedTask(IndexingTask task, String wguid);
	public void assignTasks(int count);
	
	//cluster --> id --> index name --> start offset --> end offset --> operation i.e. insert, update, or delete
	public SortedMap<Integer, IndexingTask> getAssignedTasks(String wguid, int count);
	public SortedMap<Integer, IndexingTask> getTasks(int count);
	
	//cluster --> unique worker id
	public Map<String,List<String>> getFailedWorkers();
	public void reassign();
}
