// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.sketch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class CountExact extends Sketch {

	private static final long serialVersionUID = -7751805288715115485L;
	
	public CountExact() {}
	
	public CountExact(long id, int n, int scalar, int topk, int minKey, int maxKey){
		initialize(id, n, scalar, topk, minKey, maxKey);
	}
	
	@Override
	public String getTableName() {
		return 	"skchEX" + "_" + this.n + "_" + this.scalar + "_" + this.topk;
	}
	
	public void initialize(long id, int n, int scalar, int topk, int minKey, int maxKey) {
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.id = id;
		this.n = n;
		this.scalar = scalar;
		this.topk = topk;
		map = new HashMap<Integer, long[][]>();
		long[][] counts = null;
		counts = new long[1][n];
		for (int j = 0; j < n; j++)
			counts[0][j] = 0L;
		map.put(1, counts);
	}

	@Override
	public void update(long index, double val) {
		int value = (int) Math.round(val*scalar);
		map.get(1)[0][value-1] += 1;
		
		updateStats(index, val);
	}
	
	@Override
	public void reverseUpdate(long index, double val) {
		int value = (int) Math.round(val*scalar);
		if (map.get(1)[0][value-1] > 0)
			map.get(1)[0][value-1] -= 1;
		
		reverseUpdateStats(index, val);
	}
	
	@Override
	public long pointQuery(double val) {
		int value = (int) Math.round(val*scalar);
		if (value < 1 || value > n)
			return 0;
		return map.get(1)[0][value-1];
	}
	
	@Override
	public long rangeQuery(double s, double e) {
		//dumpSketch(); // for debug log mode 
		//long start = (long) Math.round(s*scalar);
		//long end = (long) Math.round(e*scalar);
		double start = s*scalar;
		double end = e*scalar;
		long estimate = 0;
		for (double i=start; i<=end; i++) {
			//long value = (long) Math.round(i*scalar);
			estimate += pointQuery(i);
		}
		return estimate;
	}
	
	@Override
	public void setValuesFromTableName(String skchTbl) {
		//"skchEX", int n, int scalar, long id
		String[] vals = skchTbl.split("_");
		initialize(Long.parseLong(vals[4]),Integer.parseInt(vals[1]),Integer.parseInt(vals[2]),Integer.parseInt(vals[3]), -1, -1);
	}
	
	@Override
	public byte[] serializeIndex() {
		ByteArrayOutputStream fos = null;
		ObjectOutputStream out = null;
		 try
		 {
			 fos = new ByteArrayOutputStream();
			 out = new ObjectOutputStream(new DeflaterOutputStream(fos));
			 out.writeLong(minKey);
			 out.writeLong(maxKey);
			 out.writeInt(n);
			 out.writeInt(scalar);
			 out.writeInt(topk);
			 out.writeObject(heavyHitters);
			 out.writeLong(count);
			 out.writeDouble(min);
			 out.writeDouble(max);
			 out.writeDouble(first);
			 out.writeDouble(last);
			 out.writeObject(map);
			 out.flush();
			 out.close();
		 }
		 catch(IOException ex)
		 {
		   ex.printStackTrace();
		 }
		 return fos.toByteArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void deserializeIndex(byte[] idx) {
		ByteArrayInputStream fis = null;
		ObjectInputStream ois = null;
		 try
		 {
			 fis = new ByteArrayInputStream(idx);
			 ois = new ObjectInputStream(new InflaterInputStream(fis));
			 minKey = ois.readLong();
			 maxKey = ois.readLong();
			 n = ois.readInt();
			 scalar = ois.readInt();
			 topk = ois.readInt();
			 heavyHitters = (HashMap<Object,Long>) ois.readObject();
			 count = ois.readLong();
			 min = ois.readDouble();
			 max = ois.readDouble();
			 first = ois.readDouble();
			 last = ois.readDouble();
			 map = (Map<Integer, long[][]>) ois.readObject();
			 fis.close();
		 }
		 catch(IOException ex)
		 {
		   ex.printStackTrace();
		 } catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(minKey);
		out.writeLong(maxKey);
		out.writeInt(n);
		out.writeInt(scalar);
		out.writeInt(topk);
		out.writeObject(heavyHitters);
		out.writeLong(count);
		out.writeDouble(min);
		out.writeDouble(max);
		out.writeDouble(first);
		out.writeDouble(last);
		out.writeObject(map);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		minKey = in.readLong();
		maxKey = in.readLong();
		n = in.readInt();
		scalar = in.readInt();
		topk = in.readInt();
		heavyHitters = (HashMap<Object,Long>) in.readObject();
		count = in.readLong();
		min = in.readDouble();
		max = in.readDouble();
		first = in.readDouble();
		last = in.readDouble();
		map = (Map<Integer, long[][]>) in.readObject();
	}	
}