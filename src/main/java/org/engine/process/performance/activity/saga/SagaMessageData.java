package org.engine.process.performance.activity.saga;

import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.SingleMessageData;

public class SagaMessageData extends SingleMessageData {

	@Override
	public void setActivityConsumer(ActivityConsumer activityConsumer) {
		this.activityConsumer = (SagaActivityConsumer)activityConsumer;
	}

	@Override
	public String toString() {
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append("******************************************")
				.append(endl);
		stringBuffer.append("Cycle NO: " + numOfCycle).append(endl); 
		SagaActivityConsumer sagaActivityConsumer = (SagaActivityConsumer) activityConsumer;

		if (sagaActivityConsumer.getMergeTimeStamp() > 0 && 
			sagaActivityConsumer.getTimeDiff().getRight() > 0) {
			stringBuffer.append(
					"The action between topics merge and update "
							+ sagaActivityConsumer.getTimeDiff().getLeft()
							+ " millisec").append(endl);
			stringBuffer.append(
					"The action between topics split and update "
							+ sagaActivityConsumer.getTimeDiff().getRight()
							+ " millisec").append(endl);
		}
		return stringBuffer.toString();
	}
}
