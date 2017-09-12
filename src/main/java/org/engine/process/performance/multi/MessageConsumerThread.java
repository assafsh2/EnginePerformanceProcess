package org.engine.process.performance.multi;

class MessageConsumerThread implements Runnable {

	private SingleCycle singleCycle;

	public MessageConsumerThread(SingleCycle singleCycle) {

		this.singleCycle = singleCycle;
	}

	@Override
	public void run() {

		for( MessageData messageData : singleCycle.getMessageDataList()) {
	
			System.out.println("in Thread num_of_cycle "+messageData.getNumOfCycle()+" num_of_update "+messageData.getNumOfUpdate());
			
			try {
				messageData.getHandlePerformanceMessages().callConsumer();
			} catch (Exception e) { 
				
				e.printStackTrace();
			} 
		}
	}
} 