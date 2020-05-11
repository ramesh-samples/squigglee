// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.sketch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.log4j.Logger;

import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IIndexHandler;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.interfaces.IPatternHandler;

public class LocalitySensitiveHasher implements Externalizable {
	private static Logger logger = Logger.getLogger("com.squigglee.core.sketch.LocalitySensitiveHasher");
	private int replicaCount = 0;
	private int replicaNumber = 0;
	long id = 0;
	int p = 0;
	int w = 0;
	int s = 0; 
	int k = 0;
	int scalar = 0;
	LocalitySensitiveHashFunction[] hashers = null;
	//Map<String,List<Long>> lookup = new HashMap<String,List<Long>>();
	Map<Integer,List<Long>> lookup = new HashMap<Integer,List<Long>>();
	static DistanceMeasure measure = new EuclideanDistance(); 
	List<Long> candidates = null;
	String lookupKey = null;
	long minKey = 0;
	long maxKey = 0;
	
	public LocalitySensitiveHasher(int replicaCount, int replicaNumber) {
		this.replicaCount = replicaCount;
		this.replicaNumber = replicaNumber;
	}
	
	public String getIndexTableName() {
		return 	"ptrn" + "_" + this.s + "_" + this.w + "_" + this.p + "_" + this.k + "_" + this.scalar;
	}
	
	public static int getConfiguredIndexSize(String indexTblName) {
		String [] tokens = indexTblName.split("_");
		return Integer.parseInt(tokens[4]);
	}
	
	public static int getConfiguredIndexProjections(String indexTblName) {
		String [] tokens = indexTblName.split("_");
		return Integer.parseInt(tokens[3]);
	}
	
	public int getSize() { return this.s;}
	
	public Map<Integer,List<Long>> getLookupMap() {
		return this.lookup;
	}
	public void setLookupMap(Map<Integer,List<Long>> lookup) {
		this.lookup = lookup;
	}
	
	public LocalitySensitiveHashFunction[] getHashers() {
		return this.hashers;
	}
	
	public void index(long key, double[] slice) {
		if (key < minKey)
			minKey = key;
		if (key > maxKey)
			maxKey = key;
		slice = StatUtils.normalize(slice);
		
		double[] scaledSlice = new double[slice.length];
		for (int k=0; k< slice.length; k++)
			if (scalar == 1)
				scaledSlice[k] = slice[k];
			else
				scaledSlice[k] = Math.round(slice[k]*scalar);		// use scalar to restrict the cardinality of the normalized pattern 
		
		//index only the projections mapped to this local instance
		for (int i = 0; i < this.p; i++) {
			
			if (replicaNumber != i % replicaCount)
				continue;
			
			int hash = hashers[i].hash(scaledSlice);
			if (!lookup.containsKey(hash))
				lookup.put(hash, new ArrayList<Long>());
			if (!lookup.get(hash).contains(key))
				lookup.get(hash).add(key);
		}
	}
	
	public List<Long> getIndexedNeighbors(double[] pattern_norm) {
		double[] query = new double[pattern_norm.length];
		for (int k=0; k< pattern_norm.length; k++)
			if (scalar == 1)
				query[k] = pattern_norm[k];
			else
				query[k] = Math.round(pattern_norm[k]*scalar);
		candidates = new ArrayList<Long>();
		int hash;
		for (int i=0; i< this.p; i++) {
			hash = hashers[i].hash(query);
			List<Long> bucketContents = lookup.get(hash);
			if (bucketContents == null)
				continue;
			for (long c : bucketContents)
				if (!candidates.contains(c))
					candidates.add(c);
		}
		return candidates;
	}

	public List<Integer> getHashes(List<String> vals) {
		return getHashes(normalize(vals.toArray()));
	}
	
	public static double getDistance(double[] one, double[] two) {
		return measure.compute(one, two);
	}
	
	public List<Integer> getHashes(double[] norm_vals) {
		double[] query = new double[norm_vals.length];
		for (int k=0; k< norm_vals.length; k++)
			if (scalar == 1)
				query[k] = norm_vals[k];
			else
				query[k] = Math.round(norm_vals[k]*scalar);
		int hash;
		List<Integer> hashes = new ArrayList<Integer>();
		for (int i=0; i< this.p; i++) {
			hash = hashers[i].hash(query);
			hashes.add(hash);
		}
		return hashes;
	}
	
	public static double[] normalize(Collection<Object> vals) {
		return normalize(vals.toArray());
	}
	
	public static double[] normalize(Object[] vals) {
		double[] norm = new double[vals.length];
		for (int i=0; i<norm.length; i++)
			norm[i] = Double.parseDouble(vals[i].toString());
		
		return StatUtils.normalize(norm);
	}
	
	public void updateIndex(String cluster, long id, IPatternHandler handler) throws TimeSeriesException {
		String ptrnIndexTbl = getIndexTableName();
		MasterData md = ((IMasterDataHandler) handler).getMasterData(cluster, id);
		handler.updatePatternIndex(md, ptrnIndexTbl, minKey, maxKey, lookup, 0, 1);
	}
	
	public void loadSerializedIndex(String cluster, long id, String ptrnIndexTbl, IIndexHandler handler) {
		byte[] serialized;
		boolean notFound = false;
		try {
			serialized = handler.loadSerializedIndex(cluster, id, ptrnIndexTbl);
			if(serialized != null) {
				deserializeIndex(serialized); 
				//this.minKey = ((Integer) serialized[1]).intValue();
				//this.maxKey = ((Integer) serialized[2]).intValue();
				this.id = id;
			}
			else
				notFound = true;
		} catch (TimeSeriesException e) {
			notFound = true;
		}
		if (notFound) {
			setValuesFromTableName(id, ptrnIndexTbl);
			try {
				handler.saveSerializedIndex(cluster, id, ptrnIndexTbl, serializeIndex(), true);
			} catch (TimeSeriesException e) {
				logger.error("Found error saving the newly created pattern index: " + ptrnIndexTbl);
			}
		}
		//this.hashers[0].printHashFunction();
	}
		
	public byte[] serializeIndex() {
		ByteArrayOutputStream fos = null;
		ObjectOutputStream out = null;
		 try
		 {
			 fos = new ByteArrayOutputStream();
			 out = new ObjectOutputStream(new DeflaterOutputStream(fos));
			 out.writeLong(minKey);
			 out.writeLong(maxKey);
			 out.writeInt(s);
			 out.writeInt(w);
			 out.writeInt(p);
			 out.writeInt(k);
			 out.writeInt(scalar);
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
	
	public void deserializeIndex(byte[] idx) {
		ByteArrayInputStream fis = null;
		ObjectInputStream ois = null;
		 try
		 {
			 fis = new ByteArrayInputStream(idx);
			 ois = new ObjectInputStream(new InflaterInputStream(fis));
			 minKey = ois.readLong();
			 maxKey = ois.readLong();
			 s = ois.readInt();
			 w = ois.readInt();
			 p = ois.readInt();
			 k = ois.readInt();
			 scalar = ois.readInt();
			 hashers = (LocalitySensitiveHashFunction[]) ois.readObject();
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
		out.writeInt(s);
		out.writeInt(w);
		out.writeInt(p);
		out.writeInt(k);
		out.writeInt(scalar);
		out.writeObject(hashers);
		//out.writeObject(lookup);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		minKey = in.readLong();
		maxKey = in.readLong();
		s = in.readInt();
		w = in.readInt();
		p = in.readInt();
		k = in.readInt();
		scalar = in.readInt();
		hashers = (LocalitySensitiveHashFunction[]) in.readObject();
		//lookup = (Map<Integer,List<Long>>) in.readObject();
	}

	public void setValuesFromTableName(long id, String ptrnIndexTbl) {
		System.out.println("Setting values from table name = " + ptrnIndexTbl);
		//"ptrn", s, w, p, k, scalar , id 
		String[] vals = ptrnIndexTbl.split("_");
		//IndexType it = IndexType.valueOf(vals[0]);
		int size = Integer.parseInt(vals[1]);
		int width = Integer.parseInt(vals[2]);
		int projections = Integer.parseInt(vals[3]);
		int dotProducts = Integer.parseInt(vals[4]);
		int scalar = Integer.parseInt(vals[5]);
		//long id = Long.parseLong(vals[6]);
	
		initialize(id, size, width, dotProducts, projections,scalar,-1,-1);	
	}
	
	private void initialize (long id, int size, int bucketWidth, int dotProducts, int projections, int scalar, int minKey, int maxKey) {
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.id = id;
		this.s = size;
		this.w = bucketWidth;
		this.p = projections;
		this.k = dotProducts;
		this.scalar = scalar;
		hashers = new LocalitySensitiveHashFunction[this.p];
		for (int i = 0; i < projections; i++)
			hashers[i] = new LocalitySensitiveHashFunction(size, bucketWidth, dotProducts);
	}
}
