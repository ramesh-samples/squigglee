package com.squigglee.core.utility;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.config.FileShard;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesException;

public class MappedDataHandler extends MappedHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.MappedDataHandler");
	private static String storagePath = null;
	private static Map<String,FileShard> dataShards;
	private static Map<Long,Map<Integer,MappedByteBuffer>> dataBuffers = null;
	
	static {
		try {
			storagePath = LocalNodeProperties.getStoragePath();
			dataShards = new HashMap<String,FileShard>();
			//int --> 4 bytes + flag byte = 5 bytes, one billion rows; each shard 0.5 GB 
			dataShards.put("int", new FileShard(5, 10, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS*5L));
			dataShards.put("double", new FileShard(9, 10, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS*9L));
			dataShards.put("long", new FileShard(9, 10, TsrConstants.COLUMN_FAMILY_MAX_COLUMNS*9L));
			dataBuffers = new HashMap<Long,Map<Integer,MappedByteBuffer>>();
		} catch (TimeSeriesException e) {
			logger.error("Failed to execute static initializer for MappedDataHandler", e);
		}
	}
	
	public static void writeData(MasterData md, SortedMap<Long,Object> data) {
		List<Integer> updatedShards = new ArrayList<Integer>();
		int size = DataUtility.getByteSize(md.getDatatype()) + 1;
		for (Long i : data.keySet()) {
			int shardNum = dataShards.get(md.getDatatype()).getShardNumber(i);
			int index = (int) (i*size - dataShards.get(md.getDatatype()).getShardStart(i));
			put(getBuffer(md.getId(), md.getDatatype(), shardNum),index, data.get(i), md.getDatatype());
			if (!updatedShards.contains(shardNum))
				updatedShards.add(shardNum);
		}
		for (int shardNum : updatedShards)
			getBuffer(md.getId(), md.getDatatype(), shardNum).force();
	}

	public static SortedMap<Long,Object> readData(MasterData md, long start, long end) {
		SortedMap<Long,Object> results = new TreeMap<Long,Object>();
		int size = DataUtility.getByteSize(md.getDatatype()) + 1;
		for (long i=start; i<=end; i++) {
			int shardNum = dataShards.get(md.getDatatype()).getShardNumber(i);
			int index = (int) (i*size - dataShards.get(md.getDatatype()).getShardStart(i));
			results.put(i, get(getBuffer(md.getId(), md.getDatatype(), shardNum),index, md.getDatatype()));
		}
		return results;
	}
	
	private static MappedByteBuffer getBuffer(long id, String dataType, int shardNum) {
			if (!dataBuffers.containsKey(id)) {
				dataBuffers.put(id, new TreeMap<Integer,MappedByteBuffer>());
			}
			if (!dataBuffers.get(id).containsKey(shardNum)) {
				File f = new File(storagePath + "/data" + "_" + id);
				long shardSize = dataShards.get(dataType).getShardSize();
				long shardStart = shardSize*shardNum;
				dataBuffers.get(id).put(shardNum, getMappedBuffer(f, shardStart, shardSize));
			}
			return dataBuffers.get(id).get(shardNum);
	
	}
}
