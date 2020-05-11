// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.h2db;

import java.util.List;
import java.util.Map;

import com.squigglee.coord.storage.OverlayHandlerMixin;
import com.squigglee.core.interfaces.IOverlayHandler;
import com.squigglee.core.interfaces.TimeSeriesException;


public class OverlayHandlerImpl extends MasterDataHandlerImpl implements IOverlayHandler {
	//private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.OverlayHandlerImpl");
	private OverlayHandlerMixin ohMixin = null;
	
	public OverlayHandlerImpl() {
		super();
		this.ohMixin = new OverlayHandlerMixin(this.clusterName);
	}
	
	@Override
	public Map<String, Map<Integer, List<String>>> getOverlayNetwork() throws TimeSeriesException {
		return ohMixin.getOverlayNetwork();
		
		/*
		IConfig configurationService = ServiceFactory.getConfigurationService();
		Map<String, Map<Integer, List<String>>> overlayNetwork = configurationService.getOverlayNetwork(this.clusterName);
		//configurationService.close();
		return overlayNetwork;
		*/
	}
	
}
