// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.sketch;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class VectorHashFunction {
	
	//short s = (short) Random.nextInt(Short.MAX_VALUE + 1);
	int s = 32;
	int w = 8;
	short[] a = null;
	Random random = null;
	MessageDigest messageDigest = null;
	
	public VectorHashFunction() throws NoSuchAlgorithmException {
		random = new Random();
		messageDigest = MessageDigest.getInstance("SHA-256");
		initialize();
	}
	
	public int hash(Object o) {
		//TODO refactor
		byte[] raw = null;
		if (o instanceof Number)
			raw = ("" + o).getBytes();
		else 
			raw= o.toString().getBytes();
		
		messageDigest.reset();
		messageDigest.update(raw);
		return hash(messageDigest.digest());
	}
	
	public int hash(byte[] values) {
		long mod = (long) Math.pow(2, w);
		long sum = 0;
		for (int i=1; i <= s; i++) {
			sum += a[i]*values[i-1] % Math.pow(2, w*2);
		}
		return (int) ( (a[0] + sum) / mod );
	}
	
	private void initialize() {
		this.a = new short[this.s + 1];
		
		for (int i=0; i < a.length; i++) {
			a[i] = (short) random.nextInt(Short.MAX_VALUE + 1);
		}
	}
}