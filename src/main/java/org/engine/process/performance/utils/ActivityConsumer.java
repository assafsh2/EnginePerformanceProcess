package org.engine.process.performance.utils;

import org.apache.log4j.Logger;
import org.engine.process.performance.Main;

public abstract class ActivityConsumer {
	
	protected Utils utils;
	protected Logger logger = Main.logger;
	public abstract void callConsumer() throws Exception;
	
	public ActivityConsumer() {
		utils = new Utils();
	}
}
