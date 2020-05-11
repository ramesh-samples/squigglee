// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.synthetic;

import java.io.FileWriter;
import java.io.IOException;

//import java.util.Random;



import com.squigglee.core.entity.TimeSeriesException;

public class Zipf {
	int size;
	double skew;
	double c = 0.0;
			
	public Zipf() {
		this(1000000, 1.0);
	}
	
	public Zipf(int size, double skew) {
		this.size = size;
		this.skew = skew;
		normalize();
	}
	
	public int[] sample(int count) throws TimeSeriesException {
		int[] s = new int[count];
		for (int i = 0; i < count; i++)
			s[i] = sample();
		return s;
	}
	
	public boolean saveToFile(int count, String fileName) {
		FileWriter writer = null;
		try {
			writer = new FileWriter(fileName);
			int[] samples = sample(count);
			for ( int i = 0; i < samples.length ; i++)
				writer.append(samples[i] + "\n");
			writer.flush();
		} catch (Exception ex) {
			return false;
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					return false;
				}
		}
		return true;
	}
	
	public int sample() throws TimeSeriesException {
		double z = 0.0;
		do {
			z = Math.random();
		} while (z == 0);
		double sum_prob = 0;
		for (int i=1; i<=size; i++)
		  {
		    sum_prob = sum_prob + c / Math.pow(i, skew);
		    if (sum_prob >= z)
		    {
		      return i;
		      //zipf_value = i;
		      //break;
		    }
		  }
		throw new TimeSeriesException("Failed to find sample betweeen 1 and " + size);
	}
	
	private void normalize() {
	    for (int i=1; i<=size; i++)
	        c = c + (1.0 / Math.pow(i, skew));
	    c = 1.0 / c;
	}
}
