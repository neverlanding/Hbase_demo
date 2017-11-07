package com.inspur.HbasePro;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 写入hbase 内容
 * @author niubingru
 * 2017年7月10日
 */
public class HbaseStoryEntity {
	
	private String fileName;
	private long startPos;
	private long endPos;
	
	public HbaseStoryEntity(){
		
	}
	
	public HbaseStoryEntity(String fileName, long startPos, long endPos) {
		this.fileName = fileName;
		this.startPos = startPos;
		this.endPos = endPos;
	}
	
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getStartPos() {
		return startPos;
	}

	public void setStartPos(long startPos) {
		this.startPos = startPos;
	}

	public long getEndPos() {
		return endPos;
	}

	public void setEndPos(long endPos) {
		this.endPos = endPos;
	}

	/**
	 * 写入hbase的cell的数据，byte[]格式
	 * @return
	 */
	public byte[] toByte(){
		return Bytes.toBytes(fileName+"_"+startPos);
	}
}
