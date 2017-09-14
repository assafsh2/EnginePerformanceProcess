package org.engine.process.performance.multi;

import java.util.Date;

import org.apache.log4j.Logger;
import org.engine.process.performance.Main;

public class MessageData {
	
	private final Date startTime;
	private final String externalSystemID;
	private final String lat;
	private final String longX;
	private long rawDataToSourceDiffTime;
	private long sourceToUpdateDiffTime;
	private String endl = "\n";
	private String sourceName;
	private HandlePerformanceMessages handlePerformanceMessages;
	private long lastOffsetForRawData;
	private long lastOffsetForUpdate;
	private long lastOffsetForSource;
	private int numOfCycle;
	private int numOfUpdate;
	private Logger logger = Main.logger;

	public MessageData(Date startTime,String externalSystemID, String lat, String longX,String sourceName, HandlePerformanceMessages handlePerformanceMessages) {
	 
		this.startTime = startTime;
		this.externalSystemID = externalSystemID;
		this.lat = lat;
		this.longX = longX;
		this.sourceName = sourceName;
		this.handlePerformanceMessages = handlePerformanceMessages;
	} 

	public long getRawDataToSourceDiffTime() {
		return rawDataToSourceDiffTime;
	}

	public void setRawDataToSourceDiffTime(long rawDataToSourceDiffTime) {
		this.rawDataToSourceDiffTime = rawDataToSourceDiffTime;
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
		if(rawDataToSourceDiffTime < 0 || sourceToUpdateDiffTime < 0 ) {
			
			logger.error("Error - The clock is not syncronized between hosts rawDataToSourceDiffTime "+rawDataToSourceDiffTime+" sourceToUpdateDiffTime "+sourceToUpdateDiffTime);
			return null;
			
		}
		if( rawDataToSourceDiffTime >= 0 && sourceToUpdateDiffTime >= 0) {
			stringBuffer.append("lastOffsetForRawData: "+lastOffsetForRawData).append(endl);			
			stringBuffer.append("lastOffsetForSource: "+lastOffsetForSource).append(endl);	
			stringBuffer.append("lastOffsetForUpdate: "+lastOffsetForUpdate).append(endl);
			stringBuffer.append("The action between topics  <"+sourceName+"-raw-data> and <"+sourceName+"> took "+ rawDataToSourceDiffTime +" millisec").append(endl);
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
