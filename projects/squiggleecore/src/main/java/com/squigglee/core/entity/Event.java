package com.squigglee.core.entity;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.joda.time.DateTime;

public class Event {
	protected String streamId = null;
	protected long startts;
	protected int offset;
	protected Object value;
	protected long eventTime;

	public Event() {}
	
	public Event(String streamId, long startts, int offset, Object value, long eventTime) {
		this.setStreamId(streamId);
		this.setStartts(startts);
		this.setOffset(offset);
		this.setValue(value);
		this.eventTime = eventTime;
		//this.eventTime = DateTime.now(DateTimeZone.UTC).getMillis();
	}

	public String getStreamId() {
		return streamId;
	}

	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}

	public long getStartts() {
		return startts;
	}

	public void setStartts(long startts) {
		this.startts = startts;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}
	
	@Override
	public String toString() {
		return "[streamId=" + streamId + ",startts=" + startts + ",offset=" + offset + ",value=" + value  + ",eventTime=" + new DateTime(eventTime) + "]";
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(13,97).append(streamId).append(startts).append(offset).append(value).append(eventTime).toHashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Event))
			return false;
		if (obj == this)
			return true;
		Event rhs = (Event) obj;
		return new EqualsBuilder().append(this.streamId, rhs.getStreamId()).append(this.startts, rhs.getStartts()).append(this.offset, rhs.getOffset())
				.append(this.value, rhs.getValue()).append(this.eventTime, rhs.getEventTime()).isEquals();
	}
}

