// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import com.squigglee.coord.interfaces.ICEPService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.MasterData;
 
public class CEPTests extends TestBaseCEP {
	protected static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	protected static SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
	
	@org.junit.Test
	public void verifyCEP() throws Exception {
		TestUtility utility = new TestUtility();
		utility.setEnvProps("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		
		SiddhiManager siddhiManager = new SiddhiManager();
		//define stream
		siddhiManager.defineStream("define stream StockQuoteStream (symbol string, value double, time long, count long); ");
		//add CEP queries
		siddhiManager.addQuery("from StockQuoteStream[value>20] insert into HighValueQuotes;");
		//add Callbacks to see results
		siddhiManager.addCallback("HighValueQuotes", new StreamCallback() {
		     public void receive(Event[] events) {
		          EventPrinter.print(events);
		     }
		});

		//send events in to Siddhi 
		InputHandler inputHandler = siddhiManager.getInputHandler("StockQuoteStream");
		inputHandler.send(new Object[]{"IBM", 34.0, System.currentTimeMillis(), 10});
	}
	
	@org.junit.Test
	public void verifyCEP2() throws Exception {
		ICEPService cepService = ServiceFactory.getCEPService();
		List<com.squigglee.core.entity.Event> events = cepService.getEvents("TestCluster", 0, stream3.getId(), 100, true);
		for (com.squigglee.core.entity.Event event : events)
			System.out.println(event);
		
		System.out.println("Number of events = " + events.size());
		assertTrue(22 == events.size());
	}
	
	@org.junit.Test
	public void verifyRollups() throws Exception {
		List<Integer> zipfData = getSampleZipfData();
		SortedMap<Long, Object> samples = new TreeMap<Long, Object>();
		for (int i=0; i< zipfData.size(); i++)
			samples.put(new Long(i), zipfData.get(i));
		
		List<MasterData> mdList = cepHandler.getMasterData(config.getCluster(), 0, config.getGuid(), config.getStartDate().getMillis(), config.getEndDate().getMillis());
		
		Map<Frequency,Map<Long, SortedMap<Long, Object>>> rollups = TimeSeriesShard.getRollups(samples, mdList.get(0));
		System.out.println(rollups);
		MasterData md = cepHandler.getMasterData(config.getCluster(), 0, config.getGuid(), config.getStartDate().getMillis());
		SortedMap<Long, Object> fetchedSamples = cepHandler.fetchTimeSeriesLimit(md, 0, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, 60000, false);
		assertEquals(samples, fetchedSamples);

	}
}