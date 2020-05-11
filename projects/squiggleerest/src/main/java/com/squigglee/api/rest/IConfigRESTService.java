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
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesConfig;

@Path("/json/tsconfig")
public interface IConfigRESTService {	
	
	@GET()
	@Produces("application/json")
	public List<TimeSeriesConfig> getConfigJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("id") String id, 
			@MatrixParam("startts") long start, @MatrixParam("endts") long end);
	
	@GET()
	@Produces("application/json")
	@Path("/masterdata")
	public List<MasterData> getMasterDataJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("id") String id, 
			@MatrixParam("startts") long start, @MatrixParam("endts") long end);
	
	@POST()
	@Path("/masterdata")
	@Consumes("application/json")
	public String getMasterDataStatusJSON(MasterData md);
	
	@GET()
	@Produces("application/json")
	@Path("/id")
	public List<TimeSeriesConfig> getConfigJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("id") String id);
	
	@GET()
	@Produces("application/json")
	@Path("/node")
	public List<TimeSeriesConfig> getConfigJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln);
	
	@GET()
	@Produces("application/json")
	@Path("/global")
	public List<TimeSeriesConfig> getGlobalConfigJSON();
	
	@POST()
	@Consumes("application/json")
	@Produces("application/json")
	public TimeSeriesConfig createConfigJSON(TimeSeriesConfig config);
	
	@PUT()
	@Consumes("application/json")
	@Produces("application/json")
	public TimeSeriesConfig updateConfigJSON(TimeSeriesConfig config);
	
	@DELETE()
	@Consumes("application/json")
	@Produces("application/json")
	public TimeSeriesConfig deleteConfigJSON(TimeSeriesConfig config);
	
	@GET()
	@Produces("application/json")
	@Path("/replica")
	public List<Integer> getReplicaSetJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln);
	
	@POST()
	@Path("/index/drop")
	@Consumes("application/json")
	public void dropIndexJSON(TimeSeriesConfig config);
	
	@POST()
	@Path("/index/add")
	@Consumes("application/json")
	public void addIndexJSON(TimeSeriesConfig config);
	
	@GET()
	@Produces("application/json")
	@Path("/overlay")
	public Map<String,Map<String, Map<Integer, List<String>>>> getOverlayNetwork();
	
	@PUT()
	@Path("setup/{cluster}")
	public Boolean setupCluster(@PathParam("cluster") String cluster);
	
	@POST()
	@Produces("application/json")
	@Path("data")
	public NodeStatus updateNode(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("addr") String addr, @MatrixParam("dataCenter") String dataCenter, 
			@MatrixParam("instanceId") String instanceId, @MatrixParam("name") String name, @MatrixParam("isBoot") boolean isBoot, @MatrixParam("isSeed") boolean isSeed, 
			@MatrixParam("replicaOf") int replicaOf, @MatrixParam("storage") int storage, @MatrixParam("stype") String stype);
	
}
