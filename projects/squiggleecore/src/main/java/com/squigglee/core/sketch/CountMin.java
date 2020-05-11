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
import java.util.List;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class CountMin extends Sketch {

	private static final long serialVersionUID = -7751805288715115485L;
	
	private int w = 65536;
	private int d = 50;
	
	public CountMin() {}
	
	public CountMin(long id, int n, int width, int depth, int scalar, int topk, int minKey, int maxKey) {
		initialize(id, n, width, depth, scalar, topk, minKey, maxKey);
	}
	
	@Override
	public String getTableName() {
		return 	"skchCM" + "_" + this.n + "_" + this.w + "_" + this.d  + "_" + this.scalar + "_" + this.topk;
	}
	
	public void initialize(long id, int n, int width, int depth, int scalar, int topk, int minKey, int maxKey) {
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.id = id;
		this.n = n;
		w = width;
		d = depth;
		this.scalar = scalar;
		this.topk = topk;
		hashers = HashFunctionGenerator.generatePairwiseUniversalHashFunctions(n, width, d);
		map = new HashMap<Integer, long[][]>();
		int maxOrder = DyadicRange.findMaxOrder(n);
		long[][] counts = null;
		for (int o = 1; o <= maxOrder; o = o*2) {
			counts = new long[d][w];
			for (int i = 0; i < d; i++)
				for (int j = 0; j < w; j++)
					counts[i][j] = 0L;
			map.put(o, counts);
		}
	}

	@Override
	public void update(long index, double val) {
		for (int i = 0 ; i < d; i++) {
			for (int order : map.keySet()) {
				int value = (int) Math.round(val*scalar);
				int dyadicValue = DyadicRange.dyadicValue(value, order);
				//int hashindex = hashers[i].hash(dyadicValue, order);
				int hashindex = hashers[i].hash(dyadicValue);
				if (hashindex < 0)
					System.out.println("Hash index is -ive for value = " + val + " and " + hashers[i].toString());
				map.get(order)[i][hashindex] += 1;
			}
		}
		
		updateStats(index, val);
	}
	
	@Override
	public void reverseUpdate(long index, double val) {
		for (int i = 0 ; i < d; i++) {
			for (int order : map.keySet()) {
				int value = (int) Math.round(val*scalar);
				int dyadicValue = DyadicRange.dyadicValue(value, order);
				//int hashindex = hashers[i].hash(dyadicValue, order);
				int hashindex = hashers[i].hash(dyadicValue);
				if (hashindex < 0)
					System.out.println("Hash index is -ive for value = " + val + " and " + hashers[i].toString());
				if (map.get(order)[i][hashindex] > 0)
					map.get(order)[i][hashindex] -= 1;
			}
		}		
		reverseUpdateStats(index, val);
	}
	
	@Override
	public long pointQuery(double val) {
		int value = (int) Math.round(val*scalar);
		if (value < 1 || value > n)
			return 0;
		//int minHashIndex = hashers[0].hash(value,1);
		int minHashIndex = hashers[0].hash(value);
		long[][] counts = map.get(1); // get the most granular sketch for point queries 
		long min = counts[0][minHashIndex];
		for (int i = 1 ; i < d; i++) {
			//int hashIndex = hashers[i].hash(value,1);
			int hashIndex = hashers[i].hash(value);
			if (counts[i][hashIndex] < min)
				min = counts[i][hashIndex];
		}
		return min;
	}
	
	@Override
	public long rangeQuery(double s, double e) {
		//dumpSketch(); // for debug log mode 
		int start = (int) Math.round(s*scalar);
		int end = (int) Math.round(e*scalar);
		List<DyadicRange> ranges = DyadicRange.getCoveringRanges(n, start, end);

		long estimate = 0;
		for (DyadicRange range: ranges) {
			long[][] counts = map.get(range.order);
			for (int dyadicValue = range.start; dyadicValue <= range.end; dyadicValue++) {
				long min = Integer.MAX_VALUE;
				for (int i = 0 ; i < d; i++) {
					//int hashIndex = hashers[i].hash(dyadicValue,range.order);
					int hashIndex = hashers[i].hash(dyadicValue);
					if (counts[i][hashIndex] < min)
						min = counts[i][hashIndex];
				}
				estimate += min;
			}
		}
		return estimate;
	}
	
	@Override
	public void setValuesFromTableName(String skchTbl) {
		//"skchCM", int n, int width, int depth, int scalar, long id
		String[] vals = skchTbl.split("_");
		initialize(Long.parseLong(vals[6]),Integer.parseInt(vals[1]),Integer.parseInt(vals[2]),
				Integer.parseInt(vals[3]),Integer.parseInt(vals[4]), Integer.parseInt(vals[5]), -1, -1);
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
			 out.writeInt(w);
			 out.writeInt(d);
			 out.writeInt(scalar);
			 out.writeInt(topk);
			 out.writeObject(heavyHitters);
			 out.writeLong(count);
			 out.writeDouble(min);
			 out.writeDouble(max);
			 out.writeDouble(first);
			 out.writeDouble(last);			 
			 out.writeObject(map);
			 out.writeObject(hashers);
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
			 w = ois.readInt();
			 d = ois.readInt();
			 scalar = ois.readInt();
			 topk = ois.readInt();
			 heavyHitters = (HashMap<Object,Long>) ois.readObject();
			 count = ois.readLong();
			 min = ois.readDouble();
			 max = ois.readDouble();
			 first = ois.readDouble();
			 last = ois.readDouble();
			 map = (Map<Integer, long[][]>) ois.readObject();
			 hashers = (UniversalHashFunction[]) ois.readObject();
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
		out.writeInt(w);
		out.writeInt(d);
		out.writeInt(scalar);
		out.writeInt(topk);
		out.writeObject(heavyHitters);
		out.writeLong(count);
		out.writeDouble(min);
		out.writeDouble(max);
		out.writeDouble(first);
		out.writeDouble(last);	
		out.writeObject(map);
		out.writeObject(hashers);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		minKey = in.readLong();
		maxKey = in.readLong();
		n = in.readInt();
		w = in.readInt();
		d = in.readInt();
		scalar = in.readInt();
		topk = in.readInt();
		heavyHitters = (HashMap<Object,Long>) in.readObject();
		count = in.readLong();
		min = in.readDouble();
		max = in.readDouble();
		first = in.readDouble();
		last = in.readDouble();
		map = (Map<Integer, long[][]>) in.readObject();
		hashers = (UniversalHashFunction[]) in.readObject();
	}	
}