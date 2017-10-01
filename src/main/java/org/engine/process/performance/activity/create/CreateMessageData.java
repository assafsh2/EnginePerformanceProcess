package org.engine.process.performance.activity.create;

import java.util.Date;

import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.SingleMessageData;

public class CreateMessageData implements SingleMessageData{
	
	private final Date startTime;
	private final String externalSystemID;
	private final String lat;
	private final String longX;
	private long rawDataToSourceDiffTime;
	private long sourceToUpdateDiffTime;
	private String endl = "\n";
	private String sourceName;
	private CreateActivityConsumer createActivityConsumer;
	private long lastOffsetForRawData;
	private long lastOffsetForUpdate;
	private long lastOffsetForSource;
	private int numOfCycle;
	private int numOfUpdate; 

	public CreateMessageData(Date startTime,String externalSystemID, String lat, String longX,String sourceName, CreateActivityConsumer createActivityConsumer) {
	 
		this.startTime = startTime;
		this.externalSystemID = externalSystemID;
		this.lat = lat;
		this.longX = longX;
		this.sourceName = sourceName;
		this.createActivityConsumer = createActivityConsumer;
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

	public CreateActivityConsumer getCreateActivityConsumer() {
		return createActivityConsumer;
	}

	public void setCreateActivityConsumer(
			CreateActivityConsumer createActivityConsumer) {
		this.createActivityConsumer = createActivityConsumer;
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

	@Override
	public ActivityConsumer getActivityConsumer() {
 
		return createActivityConsumer;
	}

	@Override
	public void setActivityConsumer(ActivityConsumer activityConsumer) {
		this.createActivityConsumer = (CreateActivityConsumer)activityConsumer;
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
		if( rawDataToSourceDiffTime > 0 ) {
			stringBuffer.append("lastOffsetForRawData: "+lastOffsetForRawData).append(endl);			
			stringBuffer.append("lastOffsetForSource: "+lastOffsetForSource).append(endl);	
			stringBuffer.append("lastOffsetForUpdate: "+lastOffsetForUpdate).append(endl);
			stringBuffer.append("The action between topics  <"+sourceName+"-row-data> and <"+sourceName+"> took "+ rawDataToSourceDiffTime +" millisec").append(endl);
			stringBuffer.append("The action between topics  <"+sourceName+"> and <update> took "+ sourceToUpdateDiffTime +" millisec").append(endl).append(endl);
		}
	    return stringBuffer.toString();
	}
}
