package com.squigglee.core.config;

public class FileShard {
	private int shardCount;
	private int dataSize;
	private long totalSize;
	
	public FileShard(int dataSize, int shardCount, long totalSize) {
		this.dataSize = dataSize;
		this.shardCount = shardCount;
		this.totalSize = totalSize;
	}
	
	public int getShardNumber(long rowNum) {
		if (rowNum > totalSize/dataSize)
			rowNum = totalSize/dataSize;
		return (int) (rowNum*dataSize / getShardSize());
	}
	
	public long getShardStart(long rowNum) {
		int shardNum = getShardNumber(rowNum);
		return shardNum*getShardSize();
	}
	
	public long getShardSize() {
		return totalSize / shardCount;
	}
	
	public int getShardCount() {
		return this.shardCount;
	}
	
	public long getTotalSize() {
		return this.totalSize;
	}
	
	public int getDataSize() {
		return this.dataSize;
	}
	
	@Override
	public String toString() {
		return "[" + this.dataSize + "," + this.shardCount + "," + this.totalSize + "]";
	}
}
