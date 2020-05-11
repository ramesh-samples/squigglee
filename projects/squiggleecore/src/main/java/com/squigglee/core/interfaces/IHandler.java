// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.interfaces;

import com.squigglee.core.entity.TimeSeriesException;

//marker data interface to serve as super interface for all Data API interfaces 
public interface IHandler {
	public void initialize() throws TimeSeriesException;
	public void reset(String dataType) throws TimeSeriesException;
	void shutdown() throws TimeSeriesException;
}
