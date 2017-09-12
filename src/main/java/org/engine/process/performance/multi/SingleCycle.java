package org.engine.process.performance.multi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class SingleCycle   {

	private List<MessageData> messageDataList;
	
	public SingleCycle() {
		 messageDataList = Collections.synchronizedList(new ArrayList<MessageData>());		
	}

	public void addMessageData(MessageData messageData) {
		messageDataList.add(messageData);		
	}

	public List<MessageData> getMessageDataList() {
		return messageDataList;
	}

	public void setMessageDataList(List<MessageData> messageDataList) {
		this.messageDataList = messageDataList;
	} 

}
