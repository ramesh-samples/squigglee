package com.squigglee.api.rest.impl;

import com.squigglee.api.rest.IOperatorRESTService;
import com.squigglee.core.entity.Operators;

public class OperatorRESTService extends RestBase implements IOperatorRESTService {
	//private static Logger logger = Logger.getLogger("com.squigglee.api.rest.OperatorRESTService");
	
	@Override
	public Operators fetchComputedTimeSeries(Operators request) {
		return operatorProxy.fetchComputedTimeSeries(request);
	}

}
