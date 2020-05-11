// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.utility;

import java.util.List;
import java.util.Map;

public class CollectionsUtility {
	
	public static void removeMin(Map<Object,Long> heavyHitters) {
		if (heavyHitters.size() == 0)
			return;
		long min = Long.MAX_VALUE;
		Object minObject = null;
		for (Object o : heavyHitters.keySet()) {
			if (heavyHitters.get(o) <= min) {
				min = heavyHitters.get(o);
				minObject = o;
			}
		}
		heavyHitters.remove(minObject);
	}
	
	public static String getCsv(List<Long> list) {
		String csv = "";
		for (long v : list) {
			csv += v + ",";
		}
		if (csv.endsWith(","))
			csv = csv.substring(0,csv.length()-1);
		
		return csv;
	}
}
