package com.squigglee.api.rest;

import java.util.SortedMap;

import javax.ws.rs.GET;
import javax.ws.rs.MatrixParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeries;

@Path("/json/synopses")
public interface ISynopsesRESTService {	
	
	@GET()
	@Produces("application/json")
	@Path("/point")
	public Long pointQuery(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, 
			@MatrixParam("id") String id, @MatrixParam("val") String val);
	
	@GET()
	@Produces("application/json")
	@Path("/range")
	public Long rangeQuery(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, 
			@MatrixParam("id") String id, @MatrixParam("startVal") String startVal, @MatrixParam("endVal") String endVal);
	
	@GET()
	@Produces("application/json")
	@Path("/inverse")
	public String inverseQuery(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, 
			@MatrixParam("id") String id, @MatrixParam("quantile") String quantile);
	
	@GET()
	@Produces("application/json")
	@Path("/sketchhist")
	SortedMap<Integer, Long> getSketchHistogram(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, 	@MatrixParam("id") String id, 
			@MatrixParam("bins") int bins);
	
	@GET()
	@Produces("application/json")
	@Path("/stats")
	Stats statistics(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("id") String id);
	
	@GET()
	@Produces("application/json")
	@Path("/sampledhist")
	SortedMap<Integer, Long> getSampledDataHistogram(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, 
			@MatrixParam("guid") String guid, @MatrixParam("start") long start, @MatrixParam("startHfOffset") int startHfOffset, @MatrixParam("end") long end, 
			@MatrixParam("endHfOffset") int endHfOffset, @MatrixParam("bins") int bins, @MatrixParam("sampleSize") int sampleSize);
	
	@GET()
	@Produces("application/json")
	@Path("/sample")
	public TimeSeries getSampledTimeSeriesJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, 
			@MatrixParam("guid") String guid, @MatrixParam("start") long start, @MatrixParam("startHfOffset") int startHfOffset, 
			@MatrixParam("end") long end, @MatrixParam("endHfOffset") int endHfOffset, @MatrixParam("sampleSize") int sampleSize);
	
}
