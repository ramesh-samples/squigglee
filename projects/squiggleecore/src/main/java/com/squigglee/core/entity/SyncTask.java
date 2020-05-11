// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.entity;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

public class SyncTask {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.interfaces.SyncTask");
	private String cluster;
	private long id;
	private long startoffset;
	private long endoffset;
	private CommandType dataOperation;
	private byte[] data;
	private String currentPath;
	private String dataHash = "";
	private static MessageDigest digest;
	
	public SyncTask(String cluster, long id, long startoffset, long endoffset, CommandType dataOperation, byte[] data) {
		this.cluster = cluster;
		this.id = id;
		this.startoffset = startoffset;
		this.endoffset = endoffset;
		this.dataOperation = dataOperation;
		this.data = data;
		if (data != null && data.length > 0) {
			digest.update(data);
			byte[] digestBytes = digest.digest();
			BigInteger bi = new BigInteger(1, digestBytes);
			this.dataHash = String.format("%0" + (digestBytes.length << 1) + "X", bi);
		}
	}
	
	static {
		try {
			digest = MessageDigest.getInstance("SHA");
		} catch (NoSuchAlgorithmException e) {
			logger.error("Could not initialize message digest to compute data hashes", e);
		}
	}
	
	public long getId() { return id;	}
	public long getStartoffset() { return startoffset; }
	public long getEndoffset() { return endoffset; }
	public CommandType getDataOperation() { return dataOperation; }
	public String getCluster() { return cluster; }
	public byte[] getData() { return data; }
	public String getCurrentPath() {return currentPath;}
	public void setCurrentPath(String currentPath) {this.currentPath = currentPath;}
	
	//http://stackoverflow.com/questions/27581/what-issues-should-be-considered-when-overriding-equals-and-hashcode-in-java
	
	@Override
	public int hashCode() {
		//if deriving: appendSuper(super.hashCode())
		return new HashCodeBuilder(13,97).append(cluster).append(id).append(startoffset).append(endoffset)
			.append(dataOperation).append(data).toHashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof SyncTask))
			return false;
		if (obj == this)
			return true;
		SyncTask rhs = (SyncTask) obj;
		//if deriving: appendSuper(super.hashCode())
		return new EqualsBuilder().append(this.cluster, rhs.getCluster()).append(this.id, rhs.getId())
				.append(this.startoffset, rhs.getStartoffset()).append(this.endoffset, rhs.getEndoffset())
				.append(this.dataOperation, rhs.getDataOperation()).append(this.data, rhs.getData()).isEquals();
	}

	@Override
	public String toString() {
		return "[cluster=" + cluster + ",id=" + id  + ",startoffset=" + startoffset + ",endoffset=" 
				+ endoffset  + ",dataOperation=" + dataOperation + ",dataHash=" + dataHash + ",currentPath=" + currentPath + "]";
	}

}
