package com.squigglee.core.utility;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;

public abstract class MappedHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.MappedHandler");
	
	protected static MappedByteBuffer getMappedBuffer(File f, long position, long size) {
		RandomAccessFile raf = null;
		FileChannel fc = null;
		MappedByteBuffer map = null;
		try {
			raf = new RandomAccessFile(f, "rw");
			fc = raf.getChannel();
			
			int maxTry = 10;
			int retry = 0;
			map = fc.map(MapMode.READ_WRITE, position, size);
			while (map == null) {
				try {
					System.out.println("Retrying to get the index map for position = " + position);
					Thread.sleep(2000);
					map = fc.map(MapMode.READ_WRITE, position, size);
				} catch (Exception e) {
					logger.error("Error sleeping while trying to retrieve the map for file = " + f.getAbsolutePath(),e);
				}
				retry++;
				if (retry == maxTry) {
					logger.error("Failed to retrieve the map for file = " + f.getAbsolutePath());
					break;
				}
			}
		} catch (IOException e) {
			logger.error("Found error getting data buffer for file = " + f==null?"":f.getAbsolutePath(), e);
		} finally {
			try {
				if (fc != null)
					fc.close();
				if (raf != null)
					raf.close();
			} catch (IOException e) {
				logger.error("Found error getting data buffer for file = " + f==null?"":f.getAbsolutePath(), e);
			}
		}
		return map;
	}
	
	protected static void put(MappedByteBuffer mbb, int index, Object val, String dataType) {
		switch(dataType) {
		case "int":
			if (val != null) {
				mbb.put(index, (byte) 1);
				mbb.putInt(index + 1, Integer.parseInt(val.toString()));
			}
		}
	}
	
	protected static Object get(MappedByteBuffer mbb, int index, String dataType) {
		switch(dataType) {
		case "int":
			if ( mbb.get(index) != (byte) 0) {
				return mbb.getInt(index+1);
			}
		}
		return null;
	}
	
}
