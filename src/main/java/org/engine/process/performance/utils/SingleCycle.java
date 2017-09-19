package org.engine.process.performance.utils;

import java.util.ArrayList; 
import java.util.Collections;
import java.util.List;

public class SingleCycle {

	private List<SingleMessageData> messageDataList;
	
	public SingleCycle() {
		 messageDataList = Collections.synchronizedList(new ArrayList<SingleMessageData>());		
	}

	public void addMessageData(SingleMessageData messageData) {
		messageDataList.add(messageData);		
	}

	public List<SingleMessageData> getMessageDataList() {
		return messageDataList;
	}

	public void setMessageDataList(List<SingleMessageData> messageDataList) {
		this.messageDataList = messageDataList;
	}  
}
