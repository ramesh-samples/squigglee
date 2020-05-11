package com.squigglee.api.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.MatrixParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import com.squigglee.core.entity.TimeSeries;

@Path("/json/timeseries")
public interface ITimeSeriesRESTService {	
	
	@POST()
	@Consumes("application/json")
	@Produces("application/json")
	public TimeSeries getTimeSeriesJSON(TimeSeries ts);

	@GET()
	@Produces("application/json")
	@Consumes("application/json")
	public TimeSeries getTimeSeriesJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("id") String id
			, @MatrixParam("start") long start, @MatrixParam("startHfOffset") int startHfOffset, @MatrixParam("end") long end, @MatrixParam("endHfOffset") int endHfOffset);
	
	@PUT()
	@Consumes("application/json")
	@Produces("application/json")
	public TimeSeries updateTimeSeriesJSON(TimeSeries ts);
	
	@PUT()
	@Consumes("application/json")
	@Produces("application/json")
	@Path("/bulk")
	public TimeSeries updateTimeSeriesBulkJSON(TimeSeries ts);
	
	@GET()
	@Produces("application/json")
	@Consumes("application/json")
	@Path("/bulk")
	public TimeSeries getTimeSeriesBulkJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("id") String id
			, @MatrixParam("start") long start, @MatrixParam("startHfOffset") int startHfOffset, @MatrixParam("end") long end, @MatrixParam("endHfOffset") int endHfOffset);
		
	@POST()
	@Produces("application/json")
	@Consumes("application/json")
	@Path("/bulk")
	public TimeSeries getTimeSeriesBulkJSON(TimeSeries ts);

	@GET()
	@Produces("application/json")
	@Path("/sequenced")
	public TimeSeries getSequencedTimeSeriesJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("id") String id
			, @MatrixParam("start") long start, @MatrixParam("startHfOffset") int startHfOffset, @MatrixParam("end") long end,  
			@MatrixParam("endHfOffset") int endHfOffset, @MatrixParam("count") int count,  @MatrixParam("last")  boolean last);
	
	@POST()
	@Produces("application/json")
	@Path("/sequenced/md")
	public TimeSeries getSequencedTimeSeriesJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("id") String id, 
			@MatrixParam("startts") long startts, @MatrixParam("startOffset") int startOffset, @MatrixParam("endOffset") int endOffset, 
			@MatrixParam("count") int count,  @MatrixParam("last")  boolean last);
}
