package com.squigglee.core.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;

public class FunctionParser {

	public static Map<String,SortedMap<Integer,List<String>>> parseVariable(Map<String,SortedMap<Integer,List<String>>> map, String function) {
		if (map == null)
			map = new HashMap<String,SortedMap<Integer,List<String>>>();

		Scanner scanner = new Scanner(function);
		scanner.useDelimiter("'");
		while (scanner.hasNext()) {
			String variable = scanner.next();
			if (scanner.hasNext())
				scanner.next();	//skip the interleaved substring
			if (variable == null || variable.length() == 0)
				continue;
			String[] tokens = variable.split("#");
			if (tokens.length != 3)
				continue;
			if (!map.containsKey(tokens[0]))
				map.put(tokens[0], new TreeMap<Integer,List<String>>());

			int ln = Integer.parseInt(tokens[1]);
			if (!map.get(tokens[0]).containsKey(ln))
				map.get(tokens[0]).put(ln, new ArrayList<String>());
			if (!map.get(tokens[0]).get(ln).contains(tokens[2]))
				map.get(tokens[0]).get(ln).add(tokens[2]);
		}
		scanner.close();
		return map;
	}
	
	public static Map<String,SortedMap<Integer,List<String>>> parseVariables(List<String> functions) {
		Map<String,SortedMap<Integer,List<String>>> map = new HashMap<String,SortedMap<Integer,List<String>>>();
		for (int i = 0; i < functions.size(); i++)
			map = parseVariable(map, functions.get(i));
		return map;
	}
}
