// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.joda.time.DateTime;

import com.squigglee.core.config.TsrConstants;

public class TimeSeriesConfig {

	private String cluster = null;
	public String getCluster() { return this.cluster; }
	public void setCluster(String cluster) { this.cluster = cluster; }
	
	private String indexes = ""; // ptrn_1_16_1000_100_8_1000,skchCM_1_3599999_8192_100_1000
	public String getIndexes() { return this.indexes;}
	public void setIndexes(String indexes) { this.indexes = indexes;}
	
	private int logicalNode; // starts at zero 
	public int getLogicalNode() { return this.logicalNode;}
	public void setLogicalNode(int logicalNode) { this.logicalNode = logicalNode;}
	
	private String guid;
	public String getGuid() { return this.guid;}
	public void setGuid(String guid) { this.guid = guid;}
	
	private long startToken;
	public long getStartToken() { return this.startToken;}
	
	private long endToken;
	public long getEndToken() { return this.endToken;}
	
	private Frequency frequency;
	public Frequency getFrequency() { return this.frequency;}
	public void setFrequency(Frequency frequency) { this.frequency = frequency;}
	
	private String datatype;
	public String getDatatype() { return this.datatype;}
	public void setDatatype(String datatype) { this.datatype = datatype;}
	
	// must be based on DateTimeZone.UTC
	private DateTime startDate;
	public DateTime getStartDate() { return this.startDate;}
	public void setStartDate(DateTime startDate) {this.startDate = startDate;}

	// must be based on DateTimeZone.UTC	
	private DateTime endDate;
	public DateTime getEndDate() { return this.endDate;}
	public void setEndDate(DateTime endDate) {this.endDate = endDate;}
	
	private String errorMessage = null;
	public String getErrorMessage() { return errorMessage; }
	public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
	
	private boolean dataLocal = false;
	public boolean isDataLocal() { return dataLocal; }
	public void setDataLocal(boolean dataLocal) { this.dataLocal = dataLocal; }
	
	private boolean rollup = false;
	public boolean isRollup() { return rollup; }
	public void setRollup(boolean rollup) { this.rollup = rollup; }
	
	public static int getLogicalNode(long id) {
		return (int) (id / TsrConstants.TOKEN_SIZE);
	}
	
	public TimeSeriesConfig() {} 
	
	public TimeSeriesConfig(String cluster, String guid, int logicalNode, Frequency frequency, String datatype, String indexes, 
			DateTime startDate, DateTime endDate) {
		this(cluster, guid, logicalNode, frequency, datatype, indexes, startDate, endDate, false);
	}
	
	public TimeSeriesConfig(String cluster, String guid, int logicalNode, Frequency frequency, String datatype, String indexes, 
			DateTime startDate, DateTime endDate, boolean rollup) {
		this.cluster = cluster;
		this.guid = guid;
		this.logicalNode = logicalNode;
		this.startToken = logicalNode*TsrConstants.TOKEN_SIZE;
		this.endToken = (logicalNode+1)*TsrConstants.TOKEN_SIZE - 1;
		this.frequency = frequency;
		this.datatype = datatype;
		this.indexes = indexes;
		this.startDate = startDate;
		this.endDate = endDate;
		this.dataLocal = false;
		this.rollup = rollup;
	}
	
	public TimeSeriesConfig(String cluster, String guid, int logicalNode, String indexes) {
		this(cluster, guid, logicalNode, Frequency.MILLIS,  "double", indexes, new DateTime(), 
				(new DateTime()).plusMinutes(1));
	}
	
	public TimeSeriesConfig clone() {
		return new TimeSeriesConfig(cluster, guid, logicalNode, frequency, datatype, indexes, 
				startDate, endDate);
	}
	
	public static List<TimeSeriesConfig> collapseTimeIntervals(List<TimeSeriesConfig> config) {
		List<TimeSeriesConfig> output = new ArrayList<TimeSeriesConfig>();
		
		Map<String,SortedMap<Long,TimeSeriesConfig>> map = new HashMap<String,SortedMap<Long,TimeSeriesConfig>>();
		
		for (TimeSeriesConfig tsc : config) {
			if (!map.containsKey(tsc.getGuid()))
				map.put(tsc.getGuid(), new TreeMap<Long,TimeSeriesConfig>());
			map.get(tsc.getGuid()).put(tsc.getStartDate().getMillis(), tsc);
		}
		
		for (String guid : map.keySet()) {
			for (Long startKey : map.get(guid).keySet()) {
				if (output.size() == 0 || !output.get(0).getGuid().equalsIgnoreCase(guid))
					output.add(0, map.get(guid).get(startKey));
				else {
					if ( (output.get(0).getEndDate().getMillis() + 1) >= map.get(guid).get(startKey).getStartDate().getMillis())
						output.get(0).setEndDate(map.get(guid).get(startKey).getEndDate());
					else
						output.add(0, map.get(guid).get(startKey));
				}
			}
		}
		return output;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof TimeSeriesConfig))
			return false;
		if (obj == this)
			return true;
		TimeSeriesConfig rhs = (TimeSeriesConfig) obj;
		//if deriving: appendSuper(super.hashCode())
		return new EqualsBuilder().append(this.guid, rhs.getGuid()).append(this.logicalNode, rhs.getLogicalNode()).append(this.frequency, rhs.getFrequency())
				.append(this.datatype, rhs.getDatatype()).append(this.indexes, rhs.getIndexes())
				.append(this.startDate.getMillis(), rhs.getStartDate().getMillis()).append(this.endDate.getMillis(), rhs.getEndDate().getMillis())
				.isEquals();
	}
	
	@Override
	public String toString() {
		return "[" + this.guid + "," + this.logicalNode + "," + this.frequency  + "," + this.datatype + "," 
				+ this.indexes + "," + this.startDate + "," + this.endDate + "," + this.isDataLocal() + "," + this.isRollup() + "]";
	}

	
}
