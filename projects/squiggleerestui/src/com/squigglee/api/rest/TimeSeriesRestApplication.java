package com.squigglee.api.rest;

import java.util.Set;
import java.util.HashSet;

import javax.ws.rs.core.Application;

import com.squigglee.api.rest.impl.ConfigRESTService;
import com.squigglee.api.rest.impl.EventRESTService;
import com.squigglee.api.rest.impl.OperatorRESTService;
import com.squigglee.api.rest.impl.PatternRESTService;
import com.squigglee.api.rest.impl.StatusRESTService;
import com.squigglee.api.rest.impl.SynopsesRESTService;
import com.squigglee.api.rest.impl.TimeSeriesRESTService;

public class TimeSeriesRestApplication extends Application {

	private Set<Object> singletons = new HashSet<Object>();
	private Set<Class<?>> empty = new HashSet<Class<?>>();
	
	public TimeSeriesRestApplication(){
		singletons.add( (ITimeSeriesRESTService) new TimeSeriesRESTService() );
		singletons.add( (IPatternRESTService) new PatternRESTService() );
		singletons.add( (IStatusRESTService) new StatusRESTService() );
		singletons.add( (IOperatorRESTService) new OperatorRESTService() );
		singletons.add( (ISynopsesRESTService) new SynopsesRESTService() );
		singletons.add( (IConfigRESTService) new ConfigRESTService() );
		//singletons.add( (IIndexSchemaRESTService) new IndexSchemaRESTService() );
		singletons.add( (IEventRESTService) new EventRESTService() );
	}
	
	@Override
	public Set<Class<?>> getClasses() {
	     return empty;
	}
	@Override
	public Set<Object> getSingletons() {
	     return singletons;
	}
}
