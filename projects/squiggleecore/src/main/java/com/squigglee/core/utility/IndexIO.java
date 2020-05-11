package com.squigglee.core.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

public class IndexIO {
	
	private static Logger logger = Logger.getLogger("com.squigglee.core.utility.IndexIO");
	protected String storagePath = null;
	protected int nthreads = 0;
	protected ExecutorService executorService;
	
	public IndexIO (int nthreads, String storagePath) {
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
	
	protected void writeIndex(String indexName, long id, String hash, List<Long> vals) {
		appendVal(getFile(indexName, id, hash), vals);
	}
	
	public List<Long> getVals(String indexName, long id, String hash) {
		File f = getFile(indexName, id, hash);
		List<Long> vals = new ArrayList<Long>();
		FileInputStream fis = null;
		FileChannel fc = null;
		try {
			fis = new FileInputStream(f);
			fc = fis.getChannel();			
			ByteBuffer buffer = ByteBuffer.allocate( (int) fc.size() );	// these are small files
			try {
				int r = fc.read(buffer);
				if (r == -1)
					return vals;
				buffer.flip();
				while (true) {
					long val = buffer.getLong();
					if (!vals.contains(val))
						vals.add(val);
				}
			} catch (BufferUnderflowException eof) {
				// do nothing
			}
		} catch (FileNotFoundException e) {
			logger.error("Found error fetching indexed values for indexName = " + indexName + " and id = " + id + " and hash = " + hash, e);
		} catch (IOException e) {
			logger.error("Found error fetching indexed values for indexName = " + indexName + " and id = " + id + " and hash = " + hash, e);
		} finally {
			try {
				if (fc != null)
					fc.close();
				if (fis != null)
					fis.close();
			} catch (IOException e) {
				logger.error("Found error fetching indexed values for indexName = " + indexName + " and id = " + id + " and hash = " + hash, e);
			}
		}
		return vals;
	}
	
	public void deleteIndex(String indexName, long id, int depth) {
		File root = new File(storagePath + "/indexes");
		deleteNextLayer(indexName, id, root, 1, depth);
	}
	
	protected void deleteNextLayer(String indexName, long id, File f, int layer, int depth) {
		if (layer > depth)
			return;
		File i_f = null;
		for (String name : f.list()) {
			i_f = new File(f.getAbsolutePath() + "/" + name);
			if (!i_f.isDirectory() && name.equalsIgnoreCase(indexName + "_" + id))
				i_f.delete();
			else if (i_f.isDirectory() && name.startsWith("#"))
				deleteNextLayer(indexName, id, i_f, (layer + 1), depth);
		}
	}
	
	public  void setupStorage(int start, int end, int depth) {
		//createNextLayer(start, end,new File(storagePath + "/indexes"), 1, depth);
		String rootPath = storagePath + "/indexes";
		createNextLayer(start, end,new File(rootPath), 1, 1);
		for (int i = start; i<= end; i++) {
			executorService.execute(getStorageCreator(rootPath + "/#" + i,start,end,depth-1));
		}
	}

	protected Runnable getStorageCreator(String path, int start, int end, int depth) {
		return new Runnable() {
			private String path = null;
			private int start, end, depth;
			@Override
			public void run() {
				createNextLayer(start, end, new File(path), 1, depth);
				System.out.println("Done with " + path);
			}
			public Runnable initialize(String path, int start, int end, int depth) {
				this.path = path; this.start = start; this.end = end; this.depth = depth;
				return this;
			}
		}.initialize(path, start, end, depth);
	}
	
	protected void createNextLayer(int start, int end, File f, int layer, int depth) {
		if (layer > depth)
			return;
		File i_f = null;
		for (int i = start; i <= end; i++) {
			i_f = new File(f.getAbsolutePath() + "/#" + i + "/dummy");
			if (!i_f.getParentFile().exists())
				i_f.getParentFile().mkdirs();
			createNextLayer(start, end, i_f.getParentFile(), (layer + 1), depth);
		}
	}
	
	protected void appendVal(File f, List<Long> vals) {
		if (f == null || vals == null || vals.size() == 0)
			return;
		FileOutputStream fos = null;
		FileChannel fc = null;
		try {
			fos = new FileOutputStream(f, true);
			fc = fos.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate( vals.size()*8 );
			for (long val : vals) {
				buffer.putLong(val);
			}
			buffer.flip();
			fc.write(buffer);
		} catch (IOException e) {
			logger.error("Found error appending vals = " + vals + " for file = " + f.getAbsolutePath(), e);
		} finally {
			try {
				if (fc != null)
					fc.close();
				if (fos != null)
					fos.close();
			} catch (IOException e) {
				logger.error("Found error appending vals = " + vals + " for file = " + f.getAbsolutePath(), e);
			}
		}
	}
	
	protected void appendVal(FileChannel fc, ByteBuffer bf, List<Long> vals) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate( vals.size()*8 );
		for (long val : vals) {
			buffer.putLong(val);
		}
		buffer.flip();
		fc.write(buffer);
	}
	
	protected  File getFile(String indexName, long id, String hash) {
		String filePath = storagePath + "/indexes";
		String[] tokens = hash.split("#");
		for (int i =0; i < tokens.length - 1; i++)
			filePath += "/#" + tokens[i] + "";
		filePath += "/" + indexName + "_" + id + "_" + tokens[tokens.length-1];
		File f = new File(filePath);
		if (!f.getParentFile().exists())
			f.getParentFile().mkdirs();
		return f;
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
				int counter = 0;
				int stored = 0;
				DateTime start = DateTime.now();
				for (String hash : indexedValues.keySet()) {
					if ( (counter % nthreads) == this.tid) {
						stored++;
						writeIndex(indexName, id, hash, indexedValues.get(hash));
					}
					counter++;
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
}
