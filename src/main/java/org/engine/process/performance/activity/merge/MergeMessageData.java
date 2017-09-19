package org.engine.process.performance.activity.merge;

import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.SingleMessageData;  

public class MergeMessageData implements SingleMessageData {
	
	private int numOfMerge;
	private MergeActivityConsumer mergeActivityConsumer;	
 
 	public int getNumOfMerge() {
		return numOfMerge;
	}
	 
	public void setNumOfMerge(int numOfMerge) {
		this.numOfMerge = numOfMerge;
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
		return "numOfMerge=" + numOfMerge;
	}  
}
