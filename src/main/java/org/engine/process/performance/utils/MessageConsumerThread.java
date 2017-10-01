package org.engine.process.performance.utils;

import org.apache.log4j.Logger; 
import org.engine.process.performance.activity.merge.MergeActivityConsumer;

public class MessageConsumerThread implements Runnable {

	private SingleCycle singleCycle;
	final private static Logger logger = Logger.getLogger(MergeActivityConsumer.class);
	static {
		Utils.setDebugLevel(logger);
	}

	public MessageConsumerThread(SingleCycle singleCycle) {

		this.singleCycle = singleCycle;
	}

	@Override
	public void run() {
		for (SingleMessageData messageData : singleCycle.getMessageDataList()) {
			logger.debug("in Thread " + messageData.toString());
			try {
				messageData.getActivityConsumer().callConsumer();
			} catch (Exception e) {

				e.printStackTrace();
			}
		}
	}
}