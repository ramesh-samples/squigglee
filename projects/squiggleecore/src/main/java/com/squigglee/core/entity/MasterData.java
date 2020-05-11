// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.entity;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class MasterData {
	private String cluster = null;
	public String getCluster() { return this.cluster; }
	private long id;
	public long getId() { return this.id;}
	public void setId(long id) { this.id = id;}
	
	private int ln;
	public int getLn() { return this.ln; }
	public void setLn(int ln) { this.ln = ln;}
	
	private String guid;
	public String getGuid() { return this.guid; }
	public void setGuid(String guid) { this.guid = guid; }

	private long startts;
	public long getStartts() { return this.startts;}
	public void setStartts(long startts) { this.startts = startts;}
	
	private Frequency freq;
	public Frequency getFreq() { return this.freq;}
	public void setFreq(Frequency freq) { this.freq = freq;}
	
	private String datatype;
	public String getDatatype() { return this.datatype; }
	public void setDatatype(String datatype) { this.datatype = datatype; }
	
	private String indexes;
	public String getIndexes() { return this.indexes; }
	public void setIndexes(String indexes) { this.indexes = indexes; }
	
	private boolean rollup = false;
	public boolean isRollup() {	return rollup; }
	public void setRollup(boolean rollup) {	this.rollup = rollup; }
	
	public MasterData() {}
	
 	public MasterData(String cluster, long id, int ln, String guid, long startts, Frequency freq, 
			String datatype, String indexes, boolean rollup) {
 		this.cluster = cluster;
		this.id = id;
		this.ln = ln;
		this.guid = guid;
		this.startts = startts;
		this.freq = freq;
		this.datatype = datatype;
		this.indexes = indexes;
		this.rollup = rollup;
	}
 	
 	public MasterData(String cluster, long id, int ln, String guid, long startts, Frequency freq, 
			String datatype, String indexes) {
 		this(cluster, id, ln, guid, startts, freq, datatype, indexes, false);
	}
	
	public Map<IndexType, Integer> getMaxIndexDimension() {
		Map<IndexType,Integer> maxdims = new HashMap<IndexType,Integer>();
		if (this.indexes != null && this.indexes.length() > 0) {
			for (String index : this.indexes.split(";")) {
				String[] tokens = index.split("_");
				IndexType it = IndexType.valueOf(tokens[0]);
				
				if (it.equals(IndexType.ptrn)) {
					int dim = Integer.parseInt(tokens[1]);
					if (!maxdims.containsKey(it))
						maxdims.put(it, dim);
					else 
						if (maxdims.get(it) < dim)
							maxdims.put(it, dim);
				} 
				else
					maxdims.put(it, 0);
			}
		}
		return maxdims;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(13,97).append(cluster).append(id).append(ln).append(guid).append(startts)
			.append(freq).append(datatype).append(indexes).append(rollup).toHashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof MasterData))
			return false;
		if (obj == this)
			return true;
		MasterData rhs = (MasterData) obj;
		//if deriving: appendSuper(super.hashCode())
		return new EqualsBuilder().append(this.cluster, rhs.getCluster()).append(this.id, rhs.getId()).append(this.ln, rhs.getLn())
				.append(this.guid, rhs.getGuid()).append(this.startts, rhs.getStartts()).append(this.freq, rhs.getFreq())
				.append(this.datatype, rhs.getDatatype()).append(this.indexes, rhs.getIndexes()).append(this.rollup, rhs.isRollup()).isEquals();
	}
	
	@Override
	public String toString() {
		return "[" + this.id + "," + this.ln + "," + this.guid  + "," + this.startts + "," + this.freq + "," 
				+ this.datatype + "," + this.indexes + "," + this.rollup + "]";
	}

}
