package org.engine.process.performance.utils;

import org.apache.log4j.Logger;
import org.engine.process.performance.Main;

public class MessageConsumerThread implements Runnable {

	private SingleCycle singleCycle;
	private Logger logger = Main.logger;

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