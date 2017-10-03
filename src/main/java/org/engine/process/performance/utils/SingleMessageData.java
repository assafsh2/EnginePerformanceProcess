package org.engine.process.performance.utils;

public abstract class SingleMessageData {
	
	protected int numOfCycle;
	protected int numOfUpdate; 
	protected String endl = "\n";
	protected ActivityConsumer activityConsumer;	
	
 	public abstract void setActivityConsumer(ActivityConsumer activityConsumer);	
  	
 	public int getNumOfCycle() {
		return numOfCycle;
	}	 
 	
	public int getNumOfUpdate() {
		return numOfUpdate;
	}

	public void setNumOfUpdate(int numOfUpdate) {
		this.numOfUpdate = numOfUpdate;
	}

	public void setNumOfCycle(int numOfCycle) {
		this.numOfCycle = numOfCycle;
	}

	public void setNumOfMerge(int numOfCycle) {
		this.numOfCycle = numOfCycle;
	}
	 
	public ActivityConsumer getActivityConsumer() { 
		return activityConsumer;
	}  	
}
