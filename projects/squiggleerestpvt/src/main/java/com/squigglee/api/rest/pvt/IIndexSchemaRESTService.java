package com.squigglee.api.rest.pvt;

import java.util.List;

import javax.ws.rs.MatrixParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/json/patternschema")
public interface IIndexSchemaRESTService {	
	
	@POST()
	@Produces("application/json")
	@Path("/delete")
	public Boolean deletePatternIndexTablesJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln,
			@MatrixParam("idList") List<Long> idList, @MatrixParam("indexName") String indexName);

	@POST()
	@Produces("application/json")
	@Path("/create")
	public Boolean createPatternIndexTablesJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, 
			@MatrixParam("id") long id, @MatrixParam("indexName") String indexName);
}
