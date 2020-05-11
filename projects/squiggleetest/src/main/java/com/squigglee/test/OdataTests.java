package com.squigglee.test;

import org.junit.Test;
import org.odata4j.consumer.ODataConsumer;
import org.odata4j.consumer.ODataConsumers;
import org.odata4j.consumer.behaviors.OClientBehaviors;
import org.odata4j.core.OEntity;
import org.odata4j.core.OQueryRequest;

public class OdataTests {

	@Test
	public void test() {
		
		ODataConsumer c = ODataConsumers.newBuilder("http://localhost:8080/odata/TimeSeriesGlobal/").setClientBehaviors(OClientBehaviors.basicAuth("user",
				"tsruserpw")).build();
		//EdmDataServices services = c.getMetadata();
		//EdmComplexType mdType = services.findEdmComplexType("MASTERDATA");
		//OQueryRequest<OEntity> results = c.getEntities("INTEGERS")
		//		.filter("LN eq 0 and ID eq 'Parameter1' and ts ge datetime'2015-04-16T00:00:00Z' and ts le datetime'2015-04-16T00:00:00Z'");

		OQueryRequest<OEntity> request = c.getEntities("MASTERDATA").filter("ID eq 'Parameter1' and LOGICALNODE eq 0");

		for (  OEntity entity : request.execute() ) {
			System.out.println(entity);
		}
		
	}

}
