package com.squigglee.api.rest;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.MatrixParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.squigglee.core.entity.Matches;
import com.squigglee.core.entity.Pattern;

@Path("/json/patterns")
public interface IPatternRESTService {	
	
	@POST()
	@Consumes("application/json")
	@Produces("application/json")
	@Path("/match")
	public Matches fetchMatchesJSON(Matches request);

	
	@GET()
	@Produces("application/json")
	public Pattern fetchStoredPatternJSON(@MatrixParam("cluster") String cluster, @MatrixParam("pguid") String pguid);
	
	
	@PUT()
	@Produces("application/json")
	@Consumes("application/json")
	public Pattern storePatternJSON(Pattern pattern);
	
	
	@POST()
	@Produces("application/json")
	public List<Pattern> fetchCapturedPatternsJSON(@PathParam("cluster") String cluster);
	
}
