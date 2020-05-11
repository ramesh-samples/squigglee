// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IStatusHandler;
import com.squigglee.storage.mbb.HandlerBase;

public class StatusHandlerMockImpl extends HandlerBase implements IStatusHandler {

	@Override
	public void initialize() {
		// TODO Auto-generated method stub
	}

	@Override
	public List<NodeStatus> fetchClusterStatus(String cluster) throws TimeSeriesException {
		Random rand = new Random();
		List<NodeStatus> mockList = new ArrayList<NodeStatus>();
		//rand.nextInt((max - min) + 1) + min
		int nodeCount = rand.nextInt((50 - 1) + 1) + 1;
		String[] dataCenters = new String[]{"ap-northeast-1","ap-southeast-1","ap-southeast-2",
				"eu-central-1","eu-west-1","sa-east-1","us-east-1","us-west-1","us-west-2"};
		String[] locations = new String[]{"Tokyo","Singapore","Sydney",
				"Frankfurt","Ireland","SaoPaulo","Virginia","California","Oregon"};
		int[] dcCounts = new int[]{0,0,0,0,0,0,0,0,0};
		NodeStatus ns = null;
		int currentRunLength = rand.nextInt(5) + 1;
		int currentRun = 0;
		int currentDataNode = 0;
		for (int i=0; i< nodeCount; i++) {
			ns = new NodeStatus();
			ns.setLogicalNumber(i);
			ns.setAddress( (rand.nextInt(999) + 1) + "." + (rand.nextInt(999) + 1) + 
					"." + (rand.nextInt(999) + 1) + "." + (rand.nextInt(999) + 1) );
			int randLoc = rand.nextInt(dataCenters.length);
			ns.setInstanceId("i-" + locations[randLoc].charAt(0) + rand.nextInt(99999));
			ns.setDataCenter(dataCenters[randLoc]);
			ns.setName(locations[randLoc] + " Node " + ++dcCounts[randLoc]);
			ns.setNodeUp(rand.nextInt(100) >= 95 ? false : true);
			ns.setOverlayUp(rand.nextInt(100) >= 95 ? false : true);
			//ns.setStorageServiceUp(rand.nextInt(100) >= 95 ? false : true);	//5% down rate
			//ns.setIndexServiceUp(rand.nextInt(100) >= 95 ? false : true);
			//ns.setOverlayServiceUp(rand.nextInt(100) >= 95 ? false : true);
			//ns.setViewUp(rand.nextInt(100) >= 95 ? false : true);
			//ns.setGlobalViewUp(rand.nextInt(100) >= 95 ? false : true);
			
			if (++currentRun > currentRunLength) {
				currentDataNode = ns.getLogicalNumber();
				currentRunLength = rand.nextInt(5) + 1;
				currentRun = 0;
			}
			ns.setReplicaOf(currentDataNode);
			
			//assume all feeds are millisecond doubles stored for a year with incremental storage of 30 bytes / event
			//assume real-time data is being stored for current year and set cost per day to $1.00 
			//int feedCount = rand.nextInt(4) + 1;
			//int feedCount = 1;
			//DateTime now =(new DateTime()); 
			//double mult = 3600*24*1000*30.0/1.0e12;
			//double cost = 1000.0; //assume e.g. some $X.00 per TB of core time series storage  
			//int seriesCount = feedCount*now.getDayOfYear();
			//int numIndexes = rand.nextInt(2) + 2; // between 2 and 4 indexes per feed
			//int numIndexes = 1; // between 2 and 4 indexes per feed
			//int indexStorageMult = rand.nextInt(81) + 20; // between 20 and 100 extra storage for indexes 
			//int indexCount = seriesCount*numIndexes;	 

			//storage in TB for a feed 
			//double dayStorage = now.getMillisOfDay()*30.0/1.0e12;
			//double weekStorage = dayStorage + (now.getDayOfWeek()-1)*mult;
			//double monthStorage = dayStorage + (now.getDayOfMonth()-1)*mult;
			//double yearStorage = dayStorage + (now.getDayOfYear()-1)*mult;

			if (i == 0)
				ns.setBootstrapNode(true);
			else
				ns.setBootstrapNode(false);
			if (dcCounts[randLoc] < 2)
				ns.setSeedNode(true);
			else
				ns.setSeedNode(false);
			mockList.add(ns);
		}
		return mockList;
	}

	@Override
	public void updateNodeStatus(String cluster, int ln) throws TimeSeriesException {
	}

	@Override
	public void updateOverlayStatus(String cluster, int ln) throws TimeSeriesException {
	}

	@Override
	public void deleteNode(String cluster, int ln) throws TimeSeriesException {
	}

	@Override
	public Map<String, List<NodeStatus>> fetchGlobalStatus()
			throws TimeSeriesException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NodeStatus fetchNodeStatus(String cluster, int ln)
			throws TimeSeriesException {
		// TODO Auto-generated method stub
		return null;
	}
}
