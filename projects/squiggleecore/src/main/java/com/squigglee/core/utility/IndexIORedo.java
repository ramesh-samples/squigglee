package com.squigglee.core.utility;

import ie.ucd.murmur.MurmurHash;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.squigglee.core.config.TsrConstants;

public class IndexIORedo {
	
	private static Logger logger = Logger.getLogger("com.squigglee.core.utility.IndexIO");
	protected String storagePath = null;
	protected int nthreads = 0;
	protected ExecutorService executorService = null;
	
	public IndexIORedo (int nthreads, String storagePath) {
		this.nthreads = nthreads;
		this.storagePath = storagePath;
		executorService = Executors.newFixedThreadPool(nthreads);
	}
	
	public void writeIndex(String indexName, long id, Map<String, List<Long>> indexedValues) {
		
		//TODO multi thread this call 
		//for (String hash : indexedValues.keySet())
		//	appendVal(getFile(indexName, id, hash), indexedValues.get(hash));
		
		for (int i =0; i < nthreads ; i++)
			executorService.execute(getTask(i, indexName, id, indexedValues));
	}
	
	public List<Long> getVals(String indexName, long id, String hash) {
		
		List<Long> vals = new ArrayList<Long>();
		RandomAccessFile raf = null;
		FileChannel fc = null;
		try {
			File f = getFile(indexName, id);
			raf = new RandomAccessFile(f,"rw");
			fc = raf.getChannel();
			long delta = ( ((long) Integer.MAX_VALUE) - ((long) Integer.MIN_VALUE) + 1L);
			long totalSize = delta*4;
			int binInterval = (int) (totalSize / 16);
			int index = MurmurHash.hash32(hash);
			long currentPosition = (((long) index) - ((long) Integer.MIN_VALUE))*4L;
			int positionIndex = (int) (currentPosition / binInterval);
			MappedByteBuffer map = fc.map(MapMode.READ_ONLY, ((long) positionIndex)*binInterval, binInterval);
			map.position( (int) (currentPosition % binInterval) );
			int val = map.getInt();
			if (val >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS) {
				vals = getMultiVals(indexName, id, (val - TsrConstants.COLUMN_FAMILY_MAX_COLUMNS));
			} else 
				if (!vals.contains(val))
					vals.add((long) val);
		} catch (FileNotFoundException e) {
			logger.error("Found error fetching indexed values for indexName = " + indexName + " and id = " + id + " and hash = " + hash, e);
		} catch (IOException e) {
			logger.error("Found error fetching indexed values for indexName = " + indexName + " and id = " + id + " and hash = " + hash, e);
		} finally {
			try {
				if (fc != null)
					fc.close();
				if (raf != null)
					raf.close();
			} catch (IOException e) {
				logger.error("Found error fetching indexed values for indexName = " + indexName + " and id = " + id + " and hash = " + hash, e);
			}
		}
		return vals;
	}
	
	private List<Long> getMultiVals(String indexName, long id, int rowid) {
		List<Long> vals = new ArrayList<Long>();
		String path = storagePath + "/indexes" + "/" + indexName + "_" + id + "_multi" ;
		File f = new File(path);
		RandomAccessFile raf = null;
		FileChannel fc = null;
		MappedByteBuffer map = null;
		try {
			raf = new RandomAccessFile(f, "rw");
			fc = raf.getChannel();
			map = fc.map(MapMode.READ_WRITE, rowid*40, 40);	//each row is 10 ints
			for (int i=0; i<10; i++) {
				long v = (long) map.getInt();
				if (v != -1 && !vals.contains(v))
					vals.add(v);
			}	
		} catch (FileNotFoundException e) {
			logger.error("Found error fetching next id for file " + f==null?"":f.getAbsolutePath(), e);
		} catch (IOException e) {
			logger.error("Found error fetching next id for file " + f==null?"":f.getAbsolutePath(), e);
		} finally {
			try {
				if (map != null)
					map.force();
				if (fc != null)
					fc.close();
				if (raf != null)
					raf.close();
			} catch (IOException e) {
				logger.error("Found error fetching next id for file " + f==null?"":f.getAbsolutePath(), e);
			}
		}
		
		return vals;
	}
	
 	public void deleteIndex(String indexName, long id, int depth) {
		//File root = new File(storagePath + "/indexes");
	}

	protected  File getFile(String indexName, long id) throws IOException {
		String filePath = storagePath + "/indexes";
		filePath += "/" + indexName + "_" + id;
		File f = new File(filePath);
		if (!f.getParentFile().exists())
			f.getParentFile().mkdirs();
		if (!f.exists())
			preAllocateFile(f);
		return f;
	}
	
	public void preAllocateFile(File f) throws IOException {
		f.createNewFile();
		FileOutputStream fos = null;
		FileChannel fc = null;
		try {
			fos = new FileOutputStream(f, true);
			fc = fos.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate( 40000000 );	//todo bigger chunks
			buffer.clear();
			int counter = 0;
			for (int i = Integer.MIN_VALUE; i <= Integer.MAX_VALUE; i++) {
				buffer.putInt(-1);
				counter++;
				if (counter == 10000000) {
					System.out.println("Current array index = " + i);
					buffer.flip();
					fc.write(buffer);
					fc.force(true);
					buffer.clear();
					counter = 0;
				}
				if (i == Integer.MAX_VALUE)
					break;
			}
			buffer.flip();
			fc.write(buffer);
			fc.force(true);
			buffer.clear();
		} catch (IOException e) {
			logger.error("Found error pre-allocating file = " + f.getAbsolutePath(), e);
		} finally {
			try {
				if (fc != null)
					fc.close();
				if (fos != null)
					fos.close();
			} catch (IOException e) {
				logger.error("Found error pre-allocating file = " + f.getAbsolutePath(), e);
			}
		}
		System.out.println("Preallocated file = " + f.getAbsolutePath() + " of size " + f.length());
	}

	public void preAllocateMultiFile(File f) throws IOException {
		f.createNewFile();
		FileOutputStream fos = null;
		FileChannel fc = null;
		try {
			fos = new FileOutputStream(f, true);
			fc = fos.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate( 400000000 );	//todo bigger chunks
			buffer.clear();
			int counter = 0;
			//for (int i = 0; i < TsrConstants.COLUMN_FAMILY_MAX_COLUMNS; i++) {
			for (int i = 0; i < 100000000; i++) {
				for (int j = 0; j<10; j++)
					buffer.putInt(-1);
				counter++;
				if (counter == 10000000) {
					System.out.println("Current array index = " + i);
					buffer.flip();
					fc.write(buffer);
					fc.force(true);
					buffer.clear();
					counter = 0;
				}
			}
			buffer.flip();
			fc.write(buffer);
			fc.force(true);
			buffer.clear();
		} catch (IOException e) {
			logger.error("Found error pre-allocating file = " + f.getAbsolutePath(), e);
		} finally {
			try {
				if (fc != null)
					fc.close();
				if (fos != null)
					fos.close();
			} catch (IOException e) {
				logger.error("Found error pre-allocating file = " + f.getAbsolutePath(), e);
			}
		}
		System.out.println("Preallocated file = " + f.getAbsolutePath() + " of size " + f.length());
	}
	
	public void preAllocateDataFile(File f, String dataType) throws IOException {
		f.createNewFile();
		FileOutputStream fos = null;
		FileChannel fc = null;
		try {
			int dataSize = DataUtility.getByteSize(dataType);
			fos = new FileOutputStream(f, true);
			fc = fos.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate( (1 + dataSize)*100000000 );	//todo bigger chunks
			buffer.clear();
			int counter = 0;
			for (int i = 0; i < TsrConstants.COLUMN_FAMILY_MAX_COLUMNS; i++) {
				//TODO fix for all data types
				buffer.put((byte) 0);
				buffer.putInt(-1);
				counter++;
				if (counter == 100000000) {
					System.out.println("Current array index = " + i);
					buffer.flip();
					fc.write(buffer);
					fc.force(true);
					buffer.clear();
					counter = 0;
				}
			}
			buffer.flip();
			fc.write(buffer);
			fc.force(true);
			buffer.clear();
		} catch (IOException e) {
			logger.error("Found error pre-allocating file = " + f.getAbsolutePath(), e);
		}finally {
			try {
				if (fc != null)
					fc.close();
				if (fos != null)
					fos.close();
			} catch (IOException e) {
				logger.error("Found error pre-allocating file = " + f.getAbsolutePath(), e);
			}
		}
		System.out.println("Preallocated file = " + f.getAbsolutePath() + " of size " + f.length());
	}
	
	protected Runnable getTask(int tid, String indexName, long id, Map<String, List<Long>> indexedValues) {
		return new Runnable() {
			int tid = 0;
			String indexName = null;
			long id = 0L;
			Map<String, List<Long>> indexedValues = null;
			
			@Override
			public void run() {
				System.out.println("Launching storage thread with id = " + tid + " for index " + indexName + " and id = " + id);
				int stored = 0;
				DateTime start = DateTime.now();
				File f = null;
				RandomAccessFile raf = null;
				FileChannel fc = null;
				MappedByteBuffer map = null;
				int nextId = getNextId(0, indexName, id);
				try {
					f = getFile(indexName, id);
					raf = new RandomAccessFile(f, "rw");
					fc = raf.getChannel();
					long delta = ( ((long) Integer.MAX_VALUE) - ((long) Integer.MIN_VALUE) + 1L);
					long totalSize = delta*4;
					int binInterval = (int) (totalSize / nthreads);
					//int binInterval = (int) (totalSize / 16);
					map = fc.map(MapMode.READ_WRITE, ((long) tid)*binInterval, binInterval);
					
					for (String hash : indexedValues.keySet()) {
						List<Long> vals = indexedValues.get(hash);
						if (vals.size() == 0)
							continue;
						
						int index = MurmurHash.hash32(hash);
						long currentPosition = (((long) index) - ((long) Integer.MIN_VALUE))*4L;
						int positionIndex = (int) (currentPosition / binInterval);
						if (positionIndex == this.tid) {
							map.position( (int) (currentPosition % binInterval) );
							int currentVal = map.getInt();
							if (vals.size() == 1 && currentVal == vals.get(0))		//skip the redundant update 
								continue;
							if (currentVal == -1 && vals.size() == 1) {
								map.position( (int) (currentPosition % binInterval) );
								map.putInt(vals.get(0).intValue()); 
								continue;
							} else {
								nextId = updateMultiFile(nextId, vals, currentVal, indexName, id);
								map.position( (int) (currentPosition % binInterval) );
								map.putInt((nextId + TsrConstants.COLUMN_FAMILY_MAX_COLUMNS));
							}
							stored++;
						}
					}
				} catch (IOException e) {
					logger.error("Found error appending thread id = " + tid + " for file = " + f==null?"":f.getAbsolutePath(), e);
				} finally {
					try {
						map.force();
						if (fc != null)
							fc.close();
						if (raf != null)
							raf.close();
					} catch (IOException e) {
						logger.error("Found error appending thread id = " + tid + " for file = " + f==null?"":f.getAbsolutePath(), e);
					}
				}
				DateTime end = DateTime.now();
				System.out.println("Completing index storage for thread = " + tid + " for index " + indexName + " and id = " + id 
						+ " with stored count = " + stored + " in time = " + (new Interval(start, end)).toDurationMillis() + " millis");
			}
			
			public Runnable initialize(int threadid, String indexname, long id, Map<String, List<Long>> indexedValues) {
				this.tid = threadid;
				this.indexName = indexname;
				this.id = id;
				this.indexedValues = indexedValues;
				return this;
			}
			
		}.initialize(tid, indexName, id, indexedValues);
	}
	
	private synchronized int updateMultiFile(int nextId, List<Long> vals, int currentVal, String indexName, long id) {
		String path = storagePath + "/indexes" + "/" + indexName + "_" + id + "_multi" ;
		File f = new File(path);
		RandomAccessFile raf = null;
		FileChannel fc = null;
		MappedByteBuffer map = null;
		nextId = getNextId(nextId, indexName, id);
		try {
			raf = new RandomAccessFile(f, "rw");
			fc = raf.getChannel();
			if (currentVal != -1 && currentVal < TsrConstants.COLUMN_FAMILY_MAX_COLUMNS && !vals.contains(currentVal))
				vals.add(0, (long) currentVal);
			if (currentVal < TsrConstants.COLUMN_FAMILY_MAX_COLUMNS) {	//bug here 
				map = fc.map(MapMode.READ_WRITE, nextId*40, 40);	//each row is 10 ints
				int counter = 0;
				for (Long v : vals) {
					map.putInt(v.intValue());
					counter++;
					if (counter == 10)
						break;
				}
			} else if (currentVal >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS) {
				// values in main index table > 1,000,000,000 represent a pointer to the multi-table
				int multiId = currentVal - TsrConstants.COLUMN_FAMILY_MAX_COLUMNS;
				map = fc.map(MapMode.READ_WRITE, multiId*40, 40);	//each row is 10 ints
				boolean changed = false;
				List<Long> storedValues = new ArrayList<Long>();
				for (int i=0; i<10; i++) {
					long v = (long) map.getInt();
					if (v != -1)
						storedValues.add(v);
				}
				if (storedValues.size() == vals.size()) {
					for (Long v : storedValues)
						if (!vals.contains(v))
							changed = true;
				}
				if (!changed)
					return nextId;
				for (Long sv : storedValues) {
					if (sv != -1 && !vals.contains(sv))
						vals.add((long) sv);
				}
				int counter = 0;
				map.position(0);
				for (Long v : vals) {
					map.putInt(v.intValue());
					counter++;
					if (counter == 10)
						break;
				}
			}
		} catch (FileNotFoundException e) {
			logger.error("Found error fetching next id for file " + f==null?"":f.getAbsolutePath(), e);
		} catch (IOException e) {
			logger.error("Found error fetching next id for file " + f==null?"":f.getAbsolutePath(), e);
		} finally {
			try {
				if (map != null)
					map.force();
				if (fc != null)
					fc.close();
				if (raf != null)
					raf.close();
			} catch (IOException e) {
				logger.error("Found error fetching next id for file " + f==null?"":f.getAbsolutePath(), e);
			}
		}
		return nextId;
	}
	
	private int getNextId(int start, String indexName, long id) {
		String path = storagePath + "/indexes" + "/" + indexName + "_" + id + "_multi" ;
		File f = new File(path);
		RandomAccessFile raf = null;
		FileChannel fc = null;
		MappedByteBuffer map = null;
		int chunk = 400000;	// scan 10,000 rows at time 
		int currentStart = start;
		int nextId = 0;
		try {
			raf = new RandomAccessFile(f, "rw");
			fc = raf.getChannel();
			long left = fc.size() - currentStart*40;
			if (left <= 0)
				return TsrConstants.COLUMN_FAMILY_MAX_COLUMNS;	// this the max pre-allocated 
			map = fc.map(MapMode.READ_WRITE, currentStart*40, Math.min(chunk,left));	//each row is 10 ints
			
			boolean found = false;
			for (int i = 0; i< 10000; i++) {
				for (int j=0; j<10; j++) {
					int val = map.getInt();
					if (j == 0 && val == -1)
						found = true;
					if (found) {
						nextId = (currentStart + i);
						break;
					}
				}
				if (found)
					break;
			}
			currentStart = currentStart + chunk;
		} catch (FileNotFoundException e) {
			logger.error("Found error fetching next id for file " + f==null?"":f.getAbsolutePath(), e);
		} catch (IOException e) {
			logger.error("Found error fetching next id for file " + f==null?"":f.getAbsolutePath(), e);
		} finally {
			try {
				if (map != null)
					map.force();
				if (fc != null)
					fc.close();
				if (raf != null)
					raf.close();
			} catch (IOException e) {
				logger.error("Found error fetching next id for file " + f==null?"":f.getAbsolutePath(), e);
			}
		}
		return nextId;
	}

	/*
	protected static Runnable getTask(int tid, String indexName, long id, Map<String, List<Long>> indexedValues) {
		return new Runnable() {
			int tid = 0;
			String indexName = null;
			long id = 0L;
			Map<String, List<Long>> indexedValues = null;
			
			@Override
			public void run() {
				System.out.println("Launching storage thread with id = " + tid + " for index " + indexName + " and id = " + id);
				int stored = 0;
				DateTime start = DateTime.now();
				MappedByteBuffer map = null;
				//int nextId = getNextId(0, indexName, id);
				for (String hash : indexedValues.keySet()) {
					List<Long> vals = indexedValues.get(hash);
					if (vals.size() == 0)
						continue;						
					int index = MurmurHash.hash32(hash);
					long actualRow = (((long) index) - ((long) Integer.MIN_VALUE));
					int shardNum = indexShard.getShardNumber(actualRow);
					if (shardNum == this.tid) {
						
						long currentPosition = actualRow*indexShard.getDataSize();
						int row = (int) (currentPosition - indexShard.getShardStart(actualRow))/indexShard.getDataSize();
						//int currentVal = -1;
						
						int maxRetry = 10;
						int retry = 0;
						while (retry <= maxRetry) {
							try {
								retry++;
								if (retry > maxRetry) {
									System.out.println("Aborting thread after failing to get map after " + maxRetry + " tries for thread = " + tid);
									return;				
								}
								map = getIndexBuffer(indexName, id, shardNum);
								map.putInt((int) (row*indexShard.getDataSize()), vals.get(0).intValue());
								break;
								//currentVal = map.getInt(row*indexShard.getDataSize());
							} catch (Exception ex) {	// sometimes throws an exception
								logger.error("Found error putting mapped value for row " + row + " and thread = " + shardNum, ex);
							//	if (indexBuffers!= null && indexBuffers.get(id) != null && indexBuffers.get(id).get(shardNum) != null)
							//		indexBuffers.get(id).get(shardNum).clear();
								System.out.println("Resetting the map after error for row " + row + " and thread = " + shardNum + " and try = " + retry);
								//map = getIndexBuffer(indexName, id, shardNum);
								//map.putInt((int) (row*indexShard.getDataSize()), vals.get(0).intValue());
								try {
									Thread.sleep(2000);
								} catch (InterruptedException e) {
									 //TODO Auto-generated catch block
									 e.printStackTrace();
								}
								//map = getIndexBuffer(indexName, id, shardNum);
								//currentVal = map.getInt(row*indexShard.getDataSize());	//bubble it up the stack
							}
						}
						
						//if (vals.size() == 1 && currentVal == vals.get(0))		//skip the redundant update 
						//	continue;
						//if (currentVal == -1 && vals.size() == 1) {
							 
						//} else {
						//	nextId = updateMultiFile(nextId, vals, currentVal, indexName, id);
						//	map.putInt((int) (row*indexShard.getDataSize()), (nextId + TsrConstants.COLUMN_FAMILY_MAX_COLUMNS));
						//}
						stored++;
						//if (stored % 100 == 0)
						//	map.force();
					}
				}
				if (map != null)
					map.force();
				DateTime end = DateTime.now();
				System.out.println("Completing index storage for thread = " + tid + " for index " + indexName + " and id = " + id 
						+ " with stored count = " + stored + " in time = " + (new Interval(start, end)).toDurationMillis() + " millis");
			}
			
			public Runnable initialize(int threadid, String indexname, long id, Map<String, List<Long>> indexedValues) {
				this.tid = threadid;
				this.indexName = indexname;
				this.id = id;
				this.indexedValues = indexedValues;
				return this;
			}
			
		}.initialize(tid, indexName, id, indexedValues);
	}
	
	
	private static synchronized int updateMultiFile(int nextId, List<Long> vals, int currentVal, String indexName, long id) {
		nextId = 0;
		MappedByteBuffer map = null;
		if (currentVal != -1 && currentVal < TsrConstants.COLUMN_FAMILY_MAX_COLUMNS && !vals.contains(currentVal))
			vals.add(0, (long) currentVal);
		if (currentVal < TsrConstants.COLUMN_FAMILY_MAX_COLUMNS) { 
			//map = fc.map(MapMode.READ_WRITE, nextId*40, 40);	//each row is 10 ints
			nextId = getNextId(nextId, indexName, id);
			long currentPosition = nextId*indexMultiShard.getDataSize();
			int shardNum = indexMultiShard.getShardNumber(nextId);
			map = getIndexMultiBuffer(indexName, id, shardNum);
			int row = (int) (currentPosition - indexMultiShard.getShardStart(nextId))/indexMultiShard.getDataSize();
			int counter = 0;
			for (Long v : vals) {
				map.putInt((int) (row*indexMultiShard.getDataSize()) + counter*4, v.intValue());
				counter++;
				if (counter == 10)
					break;
			}
		} else if (currentVal >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS) {
			// values in main index table > 1,000,000,000 represent a pointer to the multi-table
			int multiId = currentVal - TsrConstants.COLUMN_FAMILY_MAX_COLUMNS;
			long currentPosition = multiId*indexMultiShard.getDataSize();
			int shardNum = indexMultiShard.getShardNumber(multiId);
			map = getIndexMultiBuffer(indexName, id, shardNum);
			int row = (int) (currentPosition - indexMultiShard.getShardStart(multiId))/indexMultiShard.getDataSize();
			boolean changed = false;
			List<Long> storedValues = new ArrayList<Long>();
			for (int i=0; i<10; i++) {
				long v = (long) map.getInt((int) (row*indexMultiShard.getDataSize()) + i*4);
				if (v != -1)
					storedValues.add(v);
			}
			if (storedValues.size() == vals.size()) {
				for (Long v : storedValues)
					if (!vals.contains(v))
						changed = true;
			}
			if (!changed)
				return multiId;
			for (Long sv : storedValues) {
				if (sv != -1 && !vals.contains(sv))
					vals.add((long) sv);
			}
			int counter = 0;
			for (Long v : vals) {
				map.putInt((int) (row*indexMultiShard.getDataSize()) + counter*4, v.intValue());
				counter++;
				if (counter == 10)
					break;
			}
			return currentVal;
		}
		if (map != null)
			map.force();
		return nextId;
	}
	
	private synchronized static int getNextId(int startRow, String indexName, long id) {
		MappedByteBuffer map = null;
		int nextId = 0;
		int shardNum = indexMultiShard.getShardNumber(startRow);
		boolean found = false;
		int rowCount = (int) (indexMultiShard.getShardSize()/indexMultiShard.getDataSize());
		for (int k=shardNum; k<indexMultiShard.getShardCount(); k++) {
			//long currentPosition = startRow*indexMultiShard.getDataSize();
			map = getIndexMultiBuffer(indexName, id, k);
			for (int i = 0; i< rowCount; i++) {
				for (int j=0; j<10; j++) {
					int val = map.getInt((int) (i*indexMultiShard.getDataSize()) + j*4);
					if (j == 0 && val == -1)
						found = true;
					if (found) {
						nextId = (rowCount*k + i);
						break;
					}
				}
				if (found)
					break;
			}
			if (found)
				break;
		}
		if (!found) {
			int max = (int) (indexMultiShard.getTotalSize() / indexMultiShard.getDataSize());
			System.out.println("Could not find the next id, using the maximum " + max);
			nextId = max;
		}
		//System.out.println("Current nextId = " + nextId);
		return nextId;
	}
	*/
}
