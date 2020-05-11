// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.sketch;

import java.util.ArrayList;
import java.util.List;

public class DyadicRange {

	int order = 1;
	int start = 1;
	int end = 1;
	
	public DyadicRange(int start, int end, int order) {
		this.start = start;
		this.end = end;
		this.order = order;
	}
	
	public static int dyadicValue(int value, int order) {
		if (order == 1)
			return value;
		if (value % order == 0)
			return (int) Math.floor(value / order);
		else
			return (int) Math.floor(value / order) + 1;
	}
	
	// e.g. for n = 64 and query range [12,27]
	// covering ranges are -- 8:[17,24],4:[13,16],1:[12,12],2:[25,26],1:[27,27]
	public static List<DyadicRange> getCoveringRanges(int n, int start, int end) { 
		int order = DyadicRange.findMaxOrder(n);
		return getOrderRanges(n, start, end, order);
	}
	
	private static List<DyadicRange> getOrderRanges(int n, int start, int end, int order) { 
		List<DyadicRange> ranges = new ArrayList<DyadicRange>();
		if (start > end)
			return ranges;
		int orderIndexStart = DyadicRange.dyadicValue(start, order); 
		int orderIndexEnd = DyadicRange.dyadicValue(end, order); 
		int indexStart = (orderIndexStart-1) * order + 1; 
		int indexEnd = orderIndexEnd * order; 
		if (indexStart < start) 
			orderIndexStart++; 
		if (indexEnd > end)
			orderIndexEnd--; 
		if (orderIndexStart <= orderIndexEnd)
			ranges.add(new DyadicRange(orderIndexStart,orderIndexEnd,order)); 
		// call recursively to the left and right of current interval
		ranges.addAll(DyadicRange.getOrderRanges(n, start, (orderIndexStart-1) * order, order/2)); 
		ranges.addAll(DyadicRange.getOrderRanges(n, orderIndexEnd * order + 1, end, order/2 )); 
		return ranges;
	}
	
	public int getOrder() { return this.order;}
	public int getStart() { return this.start;}
	public int getEnd() { return this.end;}
	
	public int getOriginalStart() { return (this.start - 1) * this.order + 1;}
	public int getOriginalEnd() { return this.end * this.order;}
	
	
	//return closest power of 2 greater than ln(n)
	public static int findMaxOrder(int n) {
		int maxOrder = 1;
		int logN = (int) Math.ceil(Math.log(n));
		//TODO square root may be faster
		for ( int i = 1 ; i <= n; i = i*2) {
			maxOrder = i;
			if (i > logN)
				break;
		}
		return maxOrder;
	}
}
