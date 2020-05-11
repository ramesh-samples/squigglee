// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mbb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.ISchemaHandler;
import com.squigglee.core.utility.DataUtility;

public class ISchemaHandlerImpl extends HandlerBase implements ISchemaHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.ISchemaHandlerImpl");
	protected static ExecutorService executorService = null;
	
	@Override
	public boolean createSchema(MasterData md) throws TimeSeriesException {
		return createSchema(md, (int) TimeSeriesShard.getOffsetCount(md));
	}
	
	@Override
	public boolean createSchema(MasterData md, int offsetCount) throws TimeSeriesException {
		File f = new File(storagePath + "/data" + "_" + md.getId());
		if (!f.exists())
			try {
				preAllocateDataFile(f, md.getDatatype(), offsetCount);
				return true;
			} catch (IOException e) {
				logger.error("Error pre-allocating data file for id = " + md.getId());
			}
		return false;
	}
	
	@Override
	public void deleteSchema(String cluster, long id) throws TimeSeriesException {
		File f = null;
		f = new File(storagePath + "/data" + "_" + id);
		if (f.exists()) {
			f.delete();
		}
	}
	
	@Override
	public boolean deletePatternIndexTables(List<Long> list, String index) throws TimeSeriesException {
		System.out.println("Deleting pattern index tables at location " + LocalNodeProperties.getNodeLogicalNumber() + " with address " 
				+ LocalNodeProperties.getNodeAddress());
    	boolean result = false;
		try {
			for (long id : list) {
				for (String ext : new String[]{"","_multi"}) {
					File file = new File(storagePath + "/" + index + "_" + id + ext);
					if (file.exists()) {
						result = true;
						file.delete();
					}
				}
				System.out.println("Deleted index tables for index = " + index + "_" + id);
				logger.debug("Deleted index tables for index = " + index + "_" + id);
			}
		} catch (Exception e) {
			System.out.println("Failed to delete index tables for index = " + index);
			logger.debug("Failed to delete index tables for index = " + index, e);
		}
		return result;
	}
	
	@Override
	public boolean createPatternIndexTables(String cluster, long id, String idxTableName, String dataKeyspace) throws TimeSeriesException {
		System.out.println("Creating pattern index tables at location " + LocalNodeProperties.getNodeLogicalNumber() + " with address " 
				+ LocalNodeProperties.getNodeAddress());
		boolean created = false;
		executorService = Executors.newFixedThreadPool(1);	//serialize to avoid out of memory errors
		CountDownLatch cdLatch = new CountDownLatch(2);
		if (idxTableName.toLowerCase().contains("ptrn")) {
			File f = new File(storagePath + "/" + idxTableName + "_" + id);
			if (!f.exists()) {
				executorService.execute(getAsyncAllocateTask(0, f, cdLatch));
				created = true;
			}
			File f1 = new File(storagePath + "/" + idxTableName + "_" + id + "_multi");
			if (!f1.exists()) {
				executorService.execute(getAsyncAllocateTask(1, f1, cdLatch));
				created = true;
			}			
			//uncomment for synchronous execution
			//try {
			//	cdLatch.await();
			//} catch (InterruptedException e) {
			//	logger.error("Error waiting for all threads to complete pattern index file creation for index " + idxTableName + "_" + id + " in cluster " + cluster);
			//}
		}
		return created;
	}
	
	protected void preAllocateIndexFile(File f) throws IOException {
		if (f == null)
			return;
		if (!f.exists())
			f.createNewFile();
		else
			return;
		FileOutputStream fos = null;
		FileChannel fc = null;
		try {
			fos = new FileOutputStream(f, true);
			fc = fos.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate( 400000000 );
			buffer.clear();
			int counter = 0;
			for (int i = Integer.MIN_VALUE; i <= Integer.MAX_VALUE; i++) {
				buffer.putInt(-1);
				counter++;
				if (counter == 100000000) {
					System.out.println("Preallocated " + f.getAbsolutePath() + " upto array index = " + i);
					buffer.flip();
					fc.write(buffer);
					fc.force(false);
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

	protected void preAllocateMultiIndexFile(File f) throws IOException {
		if (f == null)
			return;
		if (!f.exists())
			f.createNewFile();
		else
			return;
		FileOutputStream fos = null;
		FileChannel fc = null;
		try {
			fos = new FileOutputStream(f, true);
			fc = fos.getChannel();
			
			ByteBuffer buffer = ByteBuffer.allocate( 400000000 );	
			buffer.clear();
			for (int chunk = 0; chunk < 25; chunk++) {
				buffer = ByteBuffer.allocate( 400000000 );
				for (int i = 0; i < 10000000; i++) {
					for (int j = 0; j<10; j++)
						buffer.putInt(-1);
				}
				System.out.println("Preallocated " + f.getAbsolutePath() + " upto array index = " + (chunk+1)*10000000);
				buffer.flip();
				fc.write(buffer);
				fc.force(false);
				buffer.clear();
			}
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
	
	protected void preAllocateDataFile(File f, String dataType, int offsetCount) throws IOException {
		if (f == null)
			return;
		if (!f.exists())
			f.createNewFile();
		else
			return;
		FileOutputStream fos = null;
		FileChannel fc = null;
		try {
			int dataSize = DataUtility.getByteSize(dataType);
			fos = new FileOutputStream(f, true);
			fc = fos.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate( (1 + dataSize)*100000000 );	//todo bigger chunks
			buffer.clear();
			int counter = 0;
			for (int i = 0; i < offsetCount; i++) {
				//TODO fix for all data types
				buffer.put((byte) 0);
				switch (dataType) {
				case "int":
					buffer.putInt(0);
					break;
				case "double":
					buffer.putDouble(0.0);
					break;
				case "long":
					buffer.putLong(0L);
					break;
				}
				
				counter++;
				if (counter == 100000000) {
					System.out.println("Preallocated " + f.getAbsolutePath() + " upto array index = " + i);
					buffer.flip();
					fc.write(buffer);
					fc.force(true);
					buffer.clear();
					counter = 0;
				}
			}
			buffer.flip();
			fc.write(buffer);
			fc.force(false);
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
	
	protected Runnable getAsyncFileTask(int tid, String src, String dest, CountDownLatch cdLatch) {
		return new Runnable() {
			int tid = 0;
			private String src = null;
			private String dest = null;
			private CountDownLatch cdLatch = null;
			@Override
			public void run() {
				System.out.println("Launching file copy job # " + tid + " for src = " + src + " and dest = " + dest);
				try {
					Path srcPath = FileSystems.getDefault().getPath(src);
					Path destPath = FileSystems.getDefault().getPath(dest);
					Files.copy(srcPath, destPath);
				} catch (Exception e) {
					logger.error("Error in file copy job # " + tid + " for src = " + src + " and dest = " + dest, e);
				} finally {
					cdLatch.countDown();
				}
			}
			public Runnable initialize(int tid, String src, String dest, CountDownLatch cdLatch) {
				this.tid = tid;
				this.src = src;
				this.dest = dest;
				this.cdLatch = cdLatch;
				return this;
			}
		}.initialize(tid, src, dest, cdLatch);
	}
	
	protected Runnable getAsyncAllocateTask(int tid, File f, CountDownLatch cdLatch) {
		return new Runnable() {
			int tid = 0;
			private File f = null;
			private CountDownLatch cdLatch = null;
			@Override
			public void run() {
				System.out.println("Launching file allocation job # " + tid + " for file = " + f.getAbsolutePath());
				try {
					if (tid == 0)
						preAllocateIndexFile(f);
					else
						preAllocateMultiIndexFile(f);
				} catch (Exception e) {
					logger.error("Error in file copy job # " + tid + " for file = " + f.getAbsolutePath(), e);
				} finally {
					cdLatch.countDown();
				}
			}
			public Runnable initialize(int tid, File f, CountDownLatch cdLatch) {
				this.tid = tid;
				this.f = f;
				this.cdLatch = cdLatch;
				return this;
			}
		}.initialize(tid, f, cdLatch);
	}
}
