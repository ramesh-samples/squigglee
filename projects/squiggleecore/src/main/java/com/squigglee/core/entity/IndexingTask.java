// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.entity;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class IndexingTask {

	private String cluster;
	private int destinationLn;
	private long id;
	private long startoffset;
	private long endoffset;
	private String indexName;
	private CommandType dataOperation;
	private byte[] priorData;
	private String currentPath;
	
	public IndexingTask(String cluster, int destinationLn, long id, long startoffset, long endoffset, String indexName, CommandType dataOperation) {
		this.cluster = cluster;
		this.destinationLn = destinationLn;
		this.id = id;
		this.startoffset = startoffset;
		this.endoffset = endoffset;
		this.indexName = indexName;
		this.dataOperation = dataOperation;
	}
	
	public long getId() { return id;	}
	public int getDestinationLn() { return destinationLn;	}
	public long getStartoffset() { return startoffset; }
	public long getEndoffset() { return endoffset; }
	public String getIndexName() { return indexName; }
	public CommandType getDataOperation() { return dataOperation; }
	public String getCluster() { return cluster; }
	public byte[] getPriorData() { return priorData; }
	public void setPriorData(byte[] priorData) { this.priorData = priorData; }
	public String getCurrentPath() {return currentPath;}
	public void setCurrentPath(String currentPath) {this.currentPath = currentPath;}
	
	//http://stackoverflow.com/questions/27581/what-issues-should-be-considered-when-overriding-equals-and-hashcode-in-java
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(13,97).append(cluster).append(destinationLn).append(id).append(startoffset).append(endoffset)
			.append(indexName).append(dataOperation).toHashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof IndexingTask))
			return false;
		if (obj == this)
			return true;
		IndexingTask rhs = (IndexingTask) obj;
		return new EqualsBuilder().append(this.cluster, rhs.getCluster()).append(this.destinationLn, rhs.getDestinationLn()).append(this.id, rhs.getId())
				.append(this.startoffset, rhs.getStartoffset()).append(this.endoffset, rhs.getEndoffset())
				.append(this.indexName, rhs.getIndexName()).append(this.dataOperation, rhs.getDataOperation()).isEquals();
	}

	@Override
	public String toString() {
		return "[cluster=" + cluster + ",destinationLn=" + destinationLn + ",id=" + id  + ",startoffset=" + startoffset + ",endoffset=" 
				+ endoffset  + ",indexName=" + indexName + ",dataOperation=" + dataOperation + ",currentPath=" + this.currentPath + "]";
	}

}
