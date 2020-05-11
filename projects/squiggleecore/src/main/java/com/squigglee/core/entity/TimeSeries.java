package com.squigglee.core.entity;

import java.util.SortedMap;
import java.util.TreeMap;

public class TimeSeries {
	protected String cluster;
	protected int ln;
	protected String id;
	protected long start;
	protected long end;
	protected int startHfOffset = 0;
	protected int endHfOffset = 0;
	protected String bulkData = null;
	protected String errorMessage = null;
	protected SortedMap<Long, Object> data = new TreeMap<Long,Object>();
	
	public TimeSeries() {}
	
	public TimeSeries(String cluster, int ln, String id, long start, long end) {
		this(cluster, ln, id, start, 0, end, 0, new TreeMap<Long,Object>());
	}
	
	public TimeSeries(String cluster, int ln, String id, long start, int startHfOffset, long end, int endHfOffset) {
		this(cluster, ln, id, start, startHfOffset, end, endHfOffset, new TreeMap<Long,Object>());
	}
	
	public TimeSeries(String cluster, int ln, String id, long start, long end, String bulkData) {
		this(cluster, ln, id, start, 0, end, 0, bulkData);
	}
	
	public TimeSeries(String cluster, int ln, String id, long start, int startHfOffset, long end, int endHfOffset, String bulkData) {
		this.cluster = cluster;
		this.ln = ln;
		this.id = id;
		this.start = start;
		this.startHfOffset = startHfOffset;
		this.end = end;
		this.endHfOffset = endHfOffset;
		this.bulkData = bulkData;
	}
	
	public TimeSeries(String cluster, int ln, String id, long start, long end, SortedMap<Long, Object> data) {
		this(cluster, ln, id, start, 0, end, 0, data);
	}
	
	public TimeSeries(String cluster, int ln, String id, long start, int startHfOffset, long end, int endHfOffset, SortedMap<Long, Object> data) {
		this.cluster = cluster;
		this.ln = ln;
		this.id = id;
		this.start = start;
		this.startHfOffset = startHfOffset;
		this.end = end;
		this.endHfOffset = endHfOffset;
		this.data = data;
	}
	
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	public int getLn() {
		return ln;
	}
	public void setLn(int ln) {
		this.ln = ln;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	public SortedMap<Long, Object> getData() {
		return data;
	}
	
	public void setData(SortedMap<Long, Object> data) {
		this.data = data;
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	public String getBulkData() {
		return bulkData;
	}
	public void setBulkData(String bulkData) {
		this.bulkData = bulkData;
	}

	public int getStartHfOffset() {
		return startHfOffset;
	}

	public void setStartHfOffset(int startHfOffset) {
		this.startHfOffset = startHfOffset;
	}

	public int getEndHfOffset() {
		return endHfOffset;
	}

	public void setEndHfOffset(int endHfOffset) {
		this.endHfOffset = endHfOffset;
	}
}
