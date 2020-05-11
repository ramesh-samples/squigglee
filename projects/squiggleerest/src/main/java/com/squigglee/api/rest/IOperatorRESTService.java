package com.squigglee.api.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import com.squigglee.core.entity.Operators;

@Path("/json/operators")
public interface IOperatorRESTService {	
	
	
	@POST()
	@Consumes("application/json")
	@Produces("application/json")
	@Path("/compute")
	public Operators fetchComputedTimeSeries(Operators request);
	
}
