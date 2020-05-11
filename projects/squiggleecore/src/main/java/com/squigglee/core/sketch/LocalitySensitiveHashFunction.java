// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.sketch;

import ie.ucd.murmur.MurmurHash;

import java.nio.ByteBuffer;
import java.util.Random;

public class LocalitySensitiveHashFunction implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3520169916187085725L;
	int w = 0;
	int s = 0;
	double[][] x = null;
	int b = 0;
	int k = 0;
	Random random = new Random();
	ByteBuffer bb = null;
	
	public LocalitySensitiveHashFunction() {
		
	}
	
	public LocalitySensitiveHashFunction(int size, int bucketWidth, int dotProducts) {
		this.s = size;
		this.w = bucketWidth;
		this.k = dotProducts;
		b = random.nextInt(this.w);
		x = new double[this.k][];
		for (int j = 0; j < k; j++) {
			x[j] = new double[this.s];
			for (int i=0; i< s; i++)
				x[j][i] = random.nextGaussian();
		}
	}
	
	public int hash(double[] values) {
		bb = ByteBuffer.allocate(this.k*8);
		//String hash = "";
		for ( int j =0; j<this.k; j++) {
			double sum = 0;
			for (int i=0; i<s; i++)
				sum += x[j][i]*values[i];
			//hash += ((int) Math.floor((sum + this.b)) / this.w) + "#";
			//hash += (Math.round((sum + this.b) / this.w)) + "#";
			//hash += ((sum + this.b) / this.w) + "#";
			bb.putDouble((sum + this.b) / this.w);
			//bb.putInt( ((int) Math.floor((sum + this.b)) / this.w));
		}
		return MurmurHash.hash32(bb.array(), this.k*8);
		//return MurmurHash.hash32(hash);
	}
	
	public void printHashFunction() {
		System.out.println("Hash function summary");
		System.out.println("Size = " + this.s + ", bucketWidth = " + this.w + ", dotProduct = " + this.k);
		for (int j = 0; j < k; j++) {
			for (int i=0; i< s; i++)
				System.out.print(x[j][i]);
			System.out.println();
		}
	}
}
