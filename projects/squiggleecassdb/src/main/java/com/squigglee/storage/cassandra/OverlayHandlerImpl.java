// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.cassandra;

import java.util.List;
import java.util.Map;

import com.squigglee.coord.interfaces.IConfig;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.interfaces.IOverlayHandler;
import com.squigglee.core.interfaces.TimeSeriesException;


public class OverlayHandlerImpl extends MasterDataHandlerImpl implements IOverlayHandler {
	//private static Logger logger = Logger.getLogger("com.squigglee.storage.cassandra.OverlayHandlerImpl");

	@Override
	public Map<String, Map<Integer, List<String>>> getOverlayNetwork() throws TimeSeriesException {
		IConfig configurationService = ServiceFactory.getConfigurationService();
		Map<String, Map<Integer, List<String>>> overlayNetwork = configurationService.getOverlayNetwork(this.clusterName);
		//configurationService.close();
		return overlayNetwork;
	}
	
}
