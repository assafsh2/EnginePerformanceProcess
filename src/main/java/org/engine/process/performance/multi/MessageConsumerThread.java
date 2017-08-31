package org.engine.process.performance.multi;

class MessageConsumerThread implements Runnable {

	private SingleCycle singleCycle;

	public MessageConsumerThread(SingleCycle singleCycle) {

		this.singleCycle = singleCycle;
	}

	@Override
	public void run() {

		int i = 0;
		for( MessageData messageData : singleCycle.getMessageDataList()) {

			System.out.println("\nUpdate "+i);				
			try {
				messageData.getHandlePerformanceMessages().callConsumer();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}
} 