package org.engine.process.performance.activity.split;

import org.engine.process.performance.activity.merge.MergeActivityConsumer;
import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.SingleMessageData;

public class SplitMessageData implements SingleMessageData{

	private int numOfCycle;
	private int numOfUpdate; 
	private String endl = "\n";
	private SplitActivityConsumer splitActivityConsumer;	
	
	@Override
	public ActivityConsumer getActivityConsumer() { 
		return splitActivityConsumer;
	}

	@Override
	public void setActivityConsumer(ActivityConsumer activityConsumer) {
		// TODO Auto-generated method stub
		
	}

}
