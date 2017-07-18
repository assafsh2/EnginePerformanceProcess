package org.engine.process.performance.multi;

import java.util.Date;

public class MessageData {
	
	private final Date startTime;
	private final String externalSystemID;
	private final String lat;
	private final String longX;
	private long rowDataToSourceDiffTime;
	private long sourceToUpdateDiffTime;
	private String endl = "\n";
	private String sourceName;
	private HandlePerformanceMessages handlePerformanceMessages;
	private long lastOffsetForRawData;
	private long lastOffsetForUpdate;
	private long lastOffsetForSource;
	private int numOfCycle;
	private int numOfUpdate;

	public MessageData(Date startTime,String externalSystemID, String lat, String longX,String sourceName, HandlePerformanceMessages handlePerformanceMessages) {
	 
		this.startTime = startTime;
		this.externalSystemID = externalSystemID;
		this.lat = lat;
		this.longX = longX;
		this.sourceName = sourceName;
		this.handlePerformanceMessages = handlePerformanceMessages;
	} 

	public long getRowDataToSourceDiffTime() {
		return rowDataToSourceDiffTime;
	}

	public void setRowDataToSourceDiffTime(long rowDataToSourceDiffTime) {
		this.rowDataToSourceDiffTime = rowDataToSourceDiffTime;
	}

	public long getSourceToUpdateDiffTime() {
		return sourceToUpdateDiffTime;
	}

	public void setSourceToUpdateDiffTime(long sourceToUpdateDiffTime) {
		this.sourceToUpdateDiffTime = sourceToUpdateDiffTime;
	}

	@Override
	public String toString() { 
		
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append("******************************************").append(endl);
		stringBuffer.append("StartTime: "+startTime).append(endl);
		stringBuffer.append("Cycle NO: "+numOfCycle).append(endl);
		stringBuffer.append("Update NO: "+numOfUpdate).append(endl);
		stringBuffer.append("externalSystemID: "+externalSystemID).append(endl);
		stringBuffer.append("lat: "+lat).append(endl);
		stringBuffer.append("longX: "+longX).append(endl);
		if( rowDataToSourceDiffTime > 0 ) {
			stringBuffer.append("lastOffsetForRawData: "+lastOffsetForRawData).append(endl);			
			stringBuffer.append("lastOffsetForSource: "+lastOffsetForSource).append(endl);	
			stringBuffer.append("lastOffsetForUpdate: "+lastOffsetForUpdate).append(endl);
			stringBuffer.append("The action between topics  <"+sourceName+"-row-data> and <"+sourceName+"> took "+ rowDataToSourceDiffTime +" millisec").append(endl);
			stringBuffer.append("The action between topics  <"+sourceName+"> and <update> took "+ sourceToUpdateDiffTime +" millisec").append(endl).append(endl);
		}
	    return stringBuffer.toString();
	}

	public HandlePerformanceMessages getHandlePerformanceMessages() {
		return handlePerformanceMessages;
	}

	public void setHandlePerformanceMessages(
			HandlePerformanceMessages handlePerformanceMessages) {
		this.handlePerformanceMessages = handlePerformanceMessages;
	} 
	
	public long getLastOffsetForRawData() {
		return lastOffsetForRawData;
	}

	public void setLastOffsetForRawData(long lastOffsetForRawData) {
		this.lastOffsetForRawData = lastOffsetForRawData;
	}

	public long getLastOffsetForUpdate() {
		return lastOffsetForUpdate;
	}

	public void setLastOffsetForUpdate(long lastOffsetForUpdate) {
		this.lastOffsetForUpdate = lastOffsetForUpdate;
	}

	public long getLastOffsetForSource() {
		return lastOffsetForSource;
	}

	public void setLastOffsetForSource(long lastOffsetForSource) {
		this.lastOffsetForSource = lastOffsetForSource;
	}

	public int getNumOfCycle() {
		return numOfCycle;
	}

	public void setNumOfCycle(int numOfCycle) {
		this.numOfCycle = numOfCycle;
	}

	public int getNumOfUpdate() {
		return numOfUpdate;
	}

	public void setNumOfUpdate(int numOfUpdate) {
		this.numOfUpdate = numOfUpdate;
	}
	
	
}
