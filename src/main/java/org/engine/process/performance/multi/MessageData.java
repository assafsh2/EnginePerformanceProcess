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
		stringBuffer.append("externalSystemID: "+externalSystemID).append(endl);
		stringBuffer.append("lat: "+lat).append(endl);
		stringBuffer.append("longX: "+longX).append(endl);
		stringBuffer.append("The action between topics  <"+sourceName+"-row-data> and <"+sourceName+"> took "+ rowDataToSourceDiffTime +" millisec").append(endl);
		stringBuffer.append("The action between topics  <"+sourceName+"> and <update> took "+ sourceToUpdateDiffTime +" millisec").append(endl).append(endl);

	    return stringBuffer.toString();
	}

	public HandlePerformanceMessages getHandlePerformanceMessages() {
		return handlePerformanceMessages;
	}

	public void setHandlePerformanceMessages(
			HandlePerformanceMessages handlePerformanceMessages) {
		this.handlePerformanceMessages = handlePerformanceMessages;
	} 
	
	
}