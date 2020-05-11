package com.squigglee.api.rest;

import java.util.List;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.MatrixParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.squigglee.core.entity.NodeStatus;

@Path("/json/status")
public interface IStatusRESTService {
	
	@GET()
	@Produces("application/json")
	public Map<String,List<NodeStatus>> fetchGlobalStatus();
	
	@GET()
	@Produces("application/json")
	@Path("{cluster}")
	public List<NodeStatus> fetchClusterStatus(@PathParam("cluster") String cluster);
	
	@GET()
	@Produces("application/json")
	@Path("{cluster}/{ln}")
	public NodeStatus fetchNodeStatus(@PathParam("cluster") String cluster, @PathParam("ln") int ln);
	
	@DELETE()
	@Path("delete")
	public Boolean deleteNode(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln);
	
	@PUT()
	@Produces("application/json")
	@Path("update/node")
	public NodeStatus updateNodeStatus(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln);
	
	@PUT()
	@Produces("application/json")
	@Path("update/overlay")
	public NodeStatus updateOverlayStatus(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln);
}
