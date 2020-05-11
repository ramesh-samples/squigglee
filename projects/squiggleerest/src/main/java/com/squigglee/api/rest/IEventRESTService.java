package com.squigglee.api.rest;

import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.MatrixParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import com.squigglee.core.entity.Event;
import com.squigglee.core.entity.Query;
import com.squigglee.core.entity.Stream;

@Path("/json/event")
public interface IEventRESTService {
	
	@GET()
	@Path("/stream")
	@Produces("application/json")
	public Map<String,Map<Integer,List<Stream>>> getStreams(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln);
	
	@PUT()
	@Path("/stream")
	@Consumes("application/json")
	public void addStream(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, Stream stream);
	
	@DELETE()
	@Path("/stream")
	public void removeStream(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("streamId") String streamId);
	
	@GET()
	@Path("/query")
	@Produces("application/json")
	public List<Query> getQueries(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln);
	
	@PUT()
	@Path("/query")
	@Consumes("application/json")
	public void addQuery(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, Query query);
	
	@DELETE()
	@Path("/query")
	@Consumes("application/json")
	public void removeQuery(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("queryId") String queryId);
	
	@POST()
	@Produces("application/json")
	public List<Event> getEvents(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, 
			@MatrixParam("streamId") String streamId, @MatrixParam("count") int count, @MatrixParam("last") boolean last);
	
}
