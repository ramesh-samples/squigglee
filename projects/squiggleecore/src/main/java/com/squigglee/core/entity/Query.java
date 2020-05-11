package com.squigglee.core.entity;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Query {

	private String queryText = null;
	private String cluster = null;
	private int ln;
	private String id = null;
	private String guid = null;
	private String name = null;
	
	public Query() {}
	
	public Query(String guid, String id, String cluster, int ln, String name, String queryText) {
		this.cluster = cluster;
		this.ln = ln;
		this.id = id;
		this.name = name;
		this.queryText = queryText;
	}
	
	public Query(String guid, String cluster, int ln, String name, String queryText) {
		this.cluster = cluster;
		this.ln = ln;
		this.guid = guid;
		this.id = "Q" + this.guid.replaceAll("-", "").toUpperCase();
		this.name = name;
		this.queryText = queryText;
	}
	
	public String getQueryText() {
		return queryText;
	}
	public void setQueryText(String queryText) {
		this.queryText = queryText;
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
	public void setId(String name) {
		this.id = name;
	}
	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(13,97).append(guid).append(cluster).append(ln).append(id).append(name).append(queryText).toHashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Query))
			return false;
		if (obj == this)
			return true;
		Query rhs = (Query) obj;
		return new EqualsBuilder().append(this.guid, rhs.getGuid()).append(this.cluster, rhs.getCluster()).append(this.ln, rhs.getLn())
				.append(this.id, rhs.getId()).append(this.name, rhs.getName()).append(queryText, rhs.getQueryText()).isEquals();
	}

	@Override
	public String toString() {
		return "[guid=" + guid + ",cluster=" + cluster + ",ln=" + ln + ",id=" + id  + ",name=" + name + ",queryText=" + queryText + "]";
	}
}
