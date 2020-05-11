package com.squigglee.jobs.sync;

import java.util.SortedMap;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.squigglee.core.entity.SyncTask;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.interfaces.IDataHandler;

public class DataSyncTask implements Runnable {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.sync.DataSyncTask");
	protected SyncTask task = null;
	protected SortedMap<Integer,SyncTask> runningTasks = null;
	protected int seq = 0;
	public DataSyncTask(int seq, SyncTask task, SortedMap<Integer,SyncTask> runningTasks) {
		this.task = task;
		this.runningTasks = runningTasks;
		this.seq = seq;
	}
	
	@Override
	public void run() {
		try {
			System.out.println("Running sync task " + task);
			logger.debug("Running sync task " + task);
			IDataHandler handler = HandlerFactory.getDataHandler();
			if (task == null)
				return;
			if (handler.syncData(task)) {
				System.out.println("Successfully synced data for task " + task + " at time = " + DateTime.now());
				logger.info("Successfully synced data for task " + task + " at time = " + DateTime.now());
				handler.postIndexingJobs(task);
				handler.updataSyncStatus(task);
			}
			runningTasks.remove(seq);
			handler.shutdown();
		} catch (TimeSeriesException e) {
			logger.error("Found error executing sync task " + task + " at time = " + DateTime.now(),e);
		}
	}
}
