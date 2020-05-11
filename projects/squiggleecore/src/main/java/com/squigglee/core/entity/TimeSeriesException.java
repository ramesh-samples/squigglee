// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.entity;

public class TimeSeriesException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TimeSeriesException() {

	}
	
	public TimeSeriesException(String msg) {
		super(msg);
	}

	public TimeSeriesException(String msg, Throwable t) {
		super(msg, t);
	}
	
	public TimeSeriesException(Throwable t) {
		super(t);
	}
	
}
