package org.engine.process.performance.multi;

import org.apache.log4j.Logger;
import org.engine.process.performance.Main;

class MessageConsumerThread implements Runnable {

	private SingleCycle singleCycle;
	private Logger logger = Main.logger;

	public MessageConsumerThread(SingleCycle singleCycle) {

		this.singleCycle = singleCycle;
	}

	@Override
	public void run() {

		for( MessageData messageData : singleCycle.getMessageDataList()) {
	
			logger.debug("in Thread num_of_cycle "+messageData.getNumOfCycle()+" num_of_update "+messageData.getNumOfUpdate());
			
			try {
				messageData.getHandlePerformanceMessages().callConsumer();
			} catch (Exception e) { 
				
				e.printStackTrace();
			} 
		}
	}
} 