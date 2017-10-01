package org.engine.process.performance.activity.merge;

import java.util.Arrays;

import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.SingleMessageData;  

public class MergeMessageData implements SingleMessageData {
	
	private int numOfCycle;
	private int numOfUpdate; 
	private String endl = "\n";
	private MergeActivityConsumer mergeActivityConsumer;	
 
 	public int getNumOfCycle() {
		return numOfCycle;
	}
	 
	public void setNumOfMerge(int numOfCycle) {
		this.numOfCycle = numOfCycle;
	}
	 	  
	@Override
	public ActivityConsumer getActivityConsumer() { 
		return mergeActivityConsumer;
	} 
 
	public void setActivityConsumer(ActivityConsumer activityConsumer) { 
		this.mergeActivityConsumer = (MergeActivityConsumer)activityConsumer;
	}

	@Override
	public String toString() { 
		
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append("******************************************").append(endl); 
		stringBuffer.append("Cycle NO: "+numOfCycle).append(endl);
		stringBuffer.append("Update NO: "+numOfUpdate).append(endl);
		stringBuffer.append("UUID list: "+Arrays.toString(mergeActivityConsumer.getUuidList())).append(endl);
		stringBuffer.append("The action between topics merge and update "+ mergeActivityConsumer.getTimeDiff() +" millisec").append(endl); 
		 
	    return stringBuffer.toString();
	}

	public void setNumOfUpdate(int numOfUpdate) {
		 this.numOfUpdate = numOfUpdate; 
	}

	public void setNumOfCycle(int numOfCycle) {
		this.numOfCycle = numOfCycle;
	}
}
