package org.engine.process.performance.activity.create;

import java.util.Date;

import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.SingleMessageData;

public class CreateMessageData extends SingleMessageData{
	
	private final Date startTime;
	private final String externalSystemID; 
	private long rawDataToSourceDiffTime;
	private long sourceToUpdateDiffTime;
	private String endl = "\n";
	private String sourceName; 
	private long lastOffsetForRawData;
	private long lastOffsetForUpdate;
	private long lastOffsetForSource; 
	private String identifierId;
	
	public CreateMessageData(Date startTime,String externalSystemID, String identifierId, String sourceName, CreateActivityConsumer createActivityConsumer) {
		super();
		this.startTime = startTime;
		this.externalSystemID = externalSystemID;
		this.identifierId = identifierId; 
		this.sourceName = sourceName; 
		this.activityConsumer = createActivityConsumer;
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

	public ActivityConsumer getCreateActivityConsumer() {
		return activityConsumer;
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

	@Override
	public void setActivityConsumer(ActivityConsumer activityConsumer) {
		this.activityConsumer = (CreateActivityConsumer)activityConsumer;
	}
	
	@Override
	public String toString() { 
		
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append("******************************************").append(endl);
		stringBuffer.append("StartTime: "+startTime).append(endl);
		stringBuffer.append("Cycle NO: "+numOfCycle).append(endl);
		stringBuffer.append("Update NO: "+numOfUpdate).append(endl);
		stringBuffer.append("externalSystemID: "+externalSystemID).append(endl);
		stringBuffer.append("IdentifierId: "+identifierId).append(endl); 
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
