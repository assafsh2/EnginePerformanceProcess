package org.engine.process.performance.utils;

public abstract class ActivityConsumer {
	
	protected Utils utils; 
	public abstract void callConsumer() throws Exception;
	
	public ActivityConsumer() {
		utils = new Utils();
	}
}
