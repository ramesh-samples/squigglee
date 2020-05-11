package com.squigglee.jobs.event;

import java.util.SortedMap;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.Stream;
import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.SyncTask;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.interfaces.ICEPDataHandler;

public class EventTask implements Runnable {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.sync.DataSyncTask");
	protected SyncTask task = null;
	protected SortedMap<Integer,SyncTask> runningTasks = null;
	protected int seq = 0;
	protected SiddhiManager manager = null;
	protected Stream stream = null;
	
	public EventTask(int seq, SyncTask task, SortedMap<Integer,SyncTask> runningTasks, Stream stream, SiddhiManager manager) {
		this.task = task;
		this.runningTasks = runningTasks;
		this.seq = seq;
		this.stream = stream;
		this.manager = manager;
	}
	
	@Override
	public void run() {
		try {
			System.out.println("Running CEP sync task " + task);
			logger.debug("Running CEP sync task " + task);
			ICEPDataHandler handler = HandlerFactory.getCEPDataHandler();
			if (task == null)
				return;
			
			MasterData md = handler.getMasterData(task.getCluster(), task.getId());
			if (md != null && !task.getDataOperation().equals(CommandType.DELETE)) {
				SortedMap<Long, Object> eventData = handler.fetchTimeSeries(task.getData());
				InputHandler inputHandler = manager.getInputHandler(stream.getId());
				if (eventData != null && !eventData.isEmpty()) {
					if (md.getFreq().equals(Frequency.MICROS) || md.getFreq().equals(Frequency.NANOS)) {
						for (long offset : eventData.keySet())
							inputHandler.send(new Object[]{stream.getId(), md.getStartts(), 
									TimeSeriesShard.getOffset(md.getFreq(), md.getStartts(), offset) , System.currentTimeMillis(), eventData.get(offset)});
					} else {
							for (long offset : eventData.keySet()) {
								inputHandler.send(new Object[]{md.getStartts(), 
										offset, Double.parseDouble(eventData.get(offset).toString())});
							}
						}
				}
				System.out.println("Successfully added CEP events at node = " + LocalNodeProperties.getClusterName() 
						+ " in cluster " + LocalNodeProperties.getNodeLogicalNumber() + " for task " + task + " at time = " + DateTime.now());
				logger.info("Successfully added CEP events at node = " + LocalNodeProperties.getClusterName() 
						+ " in cluster " + LocalNodeProperties.getNodeLogicalNumber() + " for task " + task + " at time = " + DateTime.now());
			}
			handler.updataCEPSyncStatus(task);
			runningTasks.remove(seq);
			handler.shutdown();
		} catch (TimeSeriesException | InterruptedException e) {
			logger.error("Found error executing sync task " + task + " at time = " + DateTime.now(),e);
		}
	}
}
