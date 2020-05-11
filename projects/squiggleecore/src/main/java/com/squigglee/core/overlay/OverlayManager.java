// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.overlay;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;
import com.squigglee.core.entity.ViewType;
import com.squigglee.core.serializers.DynamicTypeTranslator;

public class OverlayManager {
	private static Logger logger = Logger.getLogger("com.squigglee.core.overlay.OverlayManager");
 	
	public static boolean changed(Map<String,Map<String, Map<Integer, List<String>>>> a, 
			Map<String,Map<String, Map<Integer, List<String>>>> b) {
		
		if ( (a != null && b == null) || (b != null && a == null) )
			return true;
		
		for (String dataType : a.keySet()) {
			if (!b.containsKey(dataType))
				return true;
			for (String cluster : a.keySet()) {
				if (!b.get(dataType).containsKey(cluster))
					return true;
				for (Integer ln : a.get(dataType).get(cluster).keySet() ) {
					if (!b.get(dataType).get(cluster).containsKey(ln))
						return true;
					for (String guid : a.get(dataType).get(cluster).get(ln))
						if (!b.get(dataType).get(cluster).get(ln).contains(guid))
							return true;
				}
			}
		}
		return false;
	}

	public static String buildOverlayVdb(Map<String,Map<String, Map<Integer, List<String>>>> overlayNetwork) throws TimeSeriesException {
		String vdb = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \r\n";
		vdb += "<vdb name=\"TIMESERIESGLOBAL\" version=\"1\"> \r\n";
		vdb += "<description>Time Series Dynamic VDB</description> \r\n";
		for (String model : getModelDefinitions(overlayNetwork))
			vdb += model + " \r\n"; 
		vdb += "<model name=\"TimeSeriesGlobalViewModel\" type=\"VIRTUAL\"> \r\n";
		vdb += "<metadata type=\"DDL\"> \r\n";
		vdb += "<![CDATA[ \r\n";
		for (String view : getViewDefinitions(overlayNetwork))
			vdb += view + " \r\n";
		vdb += "]]> \r\n";  
		vdb += "</metadata> \r\n";
		vdb += "</model> \r\n"; 
		vdb += "<translator name=\"TeiidTranslator\" type=\"teiid\"/> \r\n";
		vdb += "</vdb> \r\n"; 
		return vdb;
	}
	
	protected static List<String> getModelDefinitions(Map<String,Map<String, Map<Integer, List<String>>>> overlayNetwork) throws TimeSeriesException {
		List<String> modelDefs = new ArrayList<String>();
		if (overlayNetwork == null)
			return modelDefs;
		Map<String, List<Integer>> clusterMap = new HashMap<String, List<Integer>>();
		for (String dataType : overlayNetwork.keySet()) {
			for (String cluster: overlayNetwork.get(dataType).keySet()) {
				if (!clusterMap.containsKey(cluster))
					clusterMap.put(cluster, new ArrayList<Integer>());
				for (int ln : overlayNetwork.get(dataType).get(cluster).keySet()) {
					if (!clusterMap.get(cluster).contains(ln)) {
						clusterMap.get(cluster).add(ln);
					}
				}
			}
		}
		//bootstrap local node in case master data is empty 
		if (clusterMap.isEmpty()) {
			clusterMap.put(LocalNodeProperties.getClusterName(), new ArrayList<Integer>());
			clusterMap.get(LocalNodeProperties.getClusterName()).add(LocalNodeProperties.getNodeLogicalNumber());
		}
		
		for (String cluster: clusterMap.keySet()) {
			for (int ln : clusterMap.get(cluster)) {
				String model = " <model name=\"TimeSeriesSourceModel_" + cluster + "_" + ln + "\" type=\"PHYSICAL\"> \r\n";
				model += "  <source connection-jndi-name=\"java:/TimeSeriesVDB_" + cluster + "_" + ln + "\" name=\"TimeSeriesVDB_" + cluster + "_" + ln 
						+ "\" translator-name=\"TeiidTranslator\"/> \r\n";
				model += " </model>";
				modelDefs.add(model);			
			}
		}
		return modelDefs;
	}
	
	protected static List<String> getViewDefinitions(Map<String,Map<String, Map<Integer, List<String>>>> overlayNetwork) throws TimeSeriesException {
		List<String> viewDefs = new ArrayList<String>();
		if (overlayNetwork == null)
			return viewDefs;
		Map<String, List<Integer>> nodeMap = new HashMap<String, List<Integer>>();
		for (String dataType : overlayNetwork.keySet()) {
			for (String cluster: overlayNetwork.get(dataType).keySet()) {
				if (!nodeMap.containsKey(cluster))
					nodeMap.put(cluster, new ArrayList<Integer>());
				int count = 0;
				String unionViewData = getUnionViewString(dataType, ViewType.DATA);			
				for (int ln : overlayNetwork.get(dataType).get(cluster).keySet()) {
					if (!nodeMap.get(cluster).contains(ln))
						nodeMap.get(cluster).add(ln);
					unionViewData += (count++ == 0)?"":" UNION ALL ";
					unionViewData += getUnionSelectString(dataType, ViewType.DATA, cluster, ln);				
				}
				viewDefs.add(unionViewData + ";");
			}
		}
		
		int count1 = 0;
		//int count2 = 0;
		String unionViewMaster = getUnionViewString(ViewType.MASTERDATA);
		//String unionViewBulk = getUnionViewString(ViewType.BULK);
		if (nodeMap == null || nodeMap.isEmpty()) {
			logger.debug("No master data, bootstrapping local node");
			System.out.println("No master data, bootstrapping local node");
			nodeMap.put(LocalNodeProperties.getClusterName(), new ArrayList<Integer>());
			nodeMap.get(LocalNodeProperties.getClusterName()).add(LocalNodeProperties.getNodeLogicalNumber());
		}
		
		for (String cluster: nodeMap.keySet()) {
			for (int ln : nodeMap.get(cluster)) {
				unionViewMaster += (count1++ == 0)?"":" UNION ALL ";
				unionViewMaster += getUnionSelectString(ViewType.MASTERDATA, cluster, ln);
				//unionViewBulk += (count2++ == 0)?"":" UNION ALL ";
				//unionViewBulk += getUnionSelectString(ViewType.BULK, cluster, ln);				
			}
		}
		viewDefs.add(unionViewMaster + ";");
		//viewDefs.add(unionViewBulk + ";");

		return viewDefs;
	}
	
	protected static String getUnionViewString(String dataType, ViewType viewType) throws TimeSeriesException {
		View view = DynamicTypeTranslator.getViewName(dataType, viewType);
		switch(viewType) {
			case DATA:
				return "CREATE VIEW " + view.name() + " (CLUST varchar(128) not null, LN integer not null, ID varchar(128) not null, TS timestamp not null, OFF long, VAL " 
				+ DynamicTypeTranslator.getInMemDataType(view) + ") OPTIONS (UPDATABLE 'TRUE') AS " ;
			case MATCH:
			case PATTERN:
			case SAMPLE:
			case SKETCH:
			case BULK:
			case MASTERDATA:
			default:
				break;
		}
		throw new TimeSeriesException("Requested data type " + viewType + " is not currently supported");
	}
	
	protected static String getUnionViewString(ViewType viewType) throws TimeSeriesException {
		switch(viewType) {
			case MASTERDATA:
				return "CREATE VIEW MASTERDATA (CLUST varchar(128) not null, LN INTEGER NOT NULL, ID VARCHAR(128) NOT NULL," + 
				" FREQ VARCHAR(128), DATATYPE VARCHAR(128), INDEXES VARCHAR(4096), STARTTS TIMESTAMP NOT NULL, ENDTS " + 
				" TIMESTAMP NOT NULL) OPTIONS (UPDATABLE 'FALSE') AS ";
			case BULK:
			case PATTERN:
			case DATA:
			case MATCH:
			case SAMPLE:
			case SKETCH:
			default:
				break;
		}		
		throw new TimeSeriesException("Requested data type " + viewType + " incorrect, must be master data view type only");
	}
	
	protected static String getUnionSelectString(String dataType, ViewType viewType, String cluster, int ln) throws TimeSeriesException {
		View view = DynamicTypeTranslator.getViewName(dataType, viewType);
		switch(viewType) {
			case DATA:
				return " SELECT " + cluster + "_" + ln + ".clust, " + cluster + "_" + ln + ".ln, " + cluster + "_" + ln + ".id, " 
						+ cluster + "_" + ln + ".ts, " + cluster + "_" + ln + ".off, " + cluster + "_" + ln + ".val" 
						+ " FROM \"TimeSeriesSourceModel_" + cluster + "_" + ln + "\".\"TIMESERIES" + 
					".TimeSeriesSourceModel.TIMESERIES.TIMESERIES." + view.name() + "\" " + cluster + "_" + ln + " WHERE " 
					+ cluster + "_" + ln + ".CLUST = '" + cluster + "' AND " + cluster + "_" + ln + ".LN = " + ln;
			case MATCH:
			case PATTERN:
			case SAMPLE:
			case SKETCH:
			case BULK:
			case MASTERDATA:
			default:
				break;
		}
		throw new TimeSeriesException("Requested data type " + viewType + " is not currently supported");
	}
	
	protected static String getUnionSelectString(String dataType, ViewType viewType, int ln) throws TimeSeriesException {
		View view = DynamicTypeTranslator.getViewName(dataType, viewType);
		switch(viewType) {
			
			case DATA:
				return " SELECT A" + ln + ".ln, A" + ln + ".id, A" + ln + ".ts, A" + ln + ".off, A" + ln + ".val" 
						+ " FROM \"TimeSeriesSourceModel" + ln + "\".\"TIMESERIES" + 
					".TimeSeriesSourceModel.TIMESERIES.TIMESERIES." + view.name() + "\" A" + ln + " WHERE A" + ln + ".LN = " + ln;
			case MATCH:
				return "SELECT A" + ln + ".LN, A" + ln + ".ID, A" + ln + ".TS, A" + ln + ".off, A" + ln + ".VAL, A" + ln + ".PID," + 
					" A" + ln + ".RANK, A" + ln + ".RADIUS FROM \"TimeSeriesSourceModel" + ln + "\".\"TIMESERIES" + 
					".TimeSeriesSourceModel.TIMESERIES.TIMESERIES." + view.name() + "\" A" + ln + " WHERE A" + ln + ".LN = " + ln;
			case PATTERN:
				return "SELECT A" + ln + ".PGUID, A" + ln + ".PINDX, A" + ln + ".VAL FROM \"TimeSeriesSourceModel" + ln + "\".\"TIMESERIES" + 
					".TimeSeriesSourceModel.TIMESERIES.TIMESERIES." + view.name() + "\" A" + ln + " WHERE A" + ln + ".LN = " + ln;
			case SAMPLE:
				return "SELECT A" + ln + ".PGUID, A" + ln + ".PINDX, A" + ln + ".VAL FROM \"TimeSeriesSourceModel" + ln + "\".\"TIMESERIES" + 
					".TimeSeriesSourceModel.TIMESERIES.TIMESERIES." + view.name() + "\" A" + ln + " WHERE A" + ln + ".LN = " + ln;
			case SKETCH:
				return "SELECT A" + ln + ".LN, A" + ln + ".ID, A" + ln + ".FREQ, A" + ln + ".VAL, A" + ln + ".ST " + 
					" FROM \"TimeSeriesSourceModel" + ln + "\".\"TIMESERIES" + 
					".TimeSeriesSourceModel.TIMESERIES.TIMESERIES." + view.name() + "\" A" + ln + " WHERE A" + ln + ".LN = " + ln;
			case BULK:
			case MASTERDATA:
			default:
				break;
		}		
		throw new TimeSeriesException("Requested data type not supported; " + viewType);
	}
	
	protected static String getUnionSelectString(ViewType viewType, String cluster, int ln) throws TimeSeriesException {
		switch(viewType) {
		case MASTERDATA:
				return " SELECT " + cluster + "_" + ln + ".CLUST, " + cluster + "_" + ln + ".LN, " + cluster + "_" + ln + ".ID, " 
				+ cluster + "_" + ln + ".FREQ, " + cluster + "_" + ln + ".DATATYPE, " 
				+ cluster + "_" + ln + ".INDEXES, " + cluster + "_" + ln + ".STARTTS," + " " + cluster + "_" + ln + ".ENDTS FROM \"TimeSeriesSourceModel_" 
				+ cluster + "_" + + ln + "\".\"TIMESERIES" + 
					".TimeSeriesSourceModel.TIMESERIES.TIMESERIES.MASTERDATA" + "\" " + cluster + "_" + ln + " WHERE " 
				+ cluster + "_" + ln + ".CLUST = '" + cluster + "' AND " + cluster + "_" + ln + ".LN = " + ln;
		case BULK:
		case DATA:
		case MATCH:
		case PATTERN:
		case SAMPLE:
		case SKETCH:
		default:
			break;
		}		
		throw new TimeSeriesException("Requested data type " + viewType + " incorrect, must be master data view type only");
	}
	
	protected static String getUnionSelectString(ViewType viewType, int ln) throws TimeSeriesException {
		switch(viewType) {
			case BULK:
				return " SELECT A" + ln + ".LN, A" + ln + ".ID, A" + ln + ".STARTDT, A" + ln + ".startoffset, A" + ln + ".ENDDT, A" + ln + ".endoffset, A" + ln + ".DATATYPE," + 
					" A" + ln + ".BDATA FROM \"TimeSeriesSourceModel" + ln + "\".\"TIMESERIES" + 
					".TimeSeriesSourceModel.TIMESERIES.TIMESERIES.BULKDATA\"" + " A" + ln + " WHERE A" + ln + ".LN = " + ln; 
			case MASTERDATA:
				return " SELECT A" + ln + ".ID, A" + ln + ".LOGICALNODE, A" + ln + ".REPLICATION, A" + ln + ".STRATEGY," + 
			" A" + ln + ".FREQUENCY, A" + ln + ".DATATYPE, A" + ln + ".INDEXES, A" + ln + ".STORAGESTART," + 
				" A" + ln + ".STORAGEEND FROM \"TimeSeriesSourceModel" + ln + "\".\"TIMESERIES" + 
					".TimeSeriesSourceModel.TIMESERIES.TIMESERIES.MASTERDATA" + "\" A" + ln + " WHERE A" + ln + ".LOGICALNODE = " + ln; 
		case DATA:
		case MATCH:
		case PATTERN:
		case SAMPLE:
		case SKETCH:
		default:
			break;
		}		
		throw new TimeSeriesException("Requested data type must be master or bulk data type only; " + viewType);
	}
}
