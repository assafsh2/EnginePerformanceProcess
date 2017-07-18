package org.engine.process.performance.multi;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.engine.process.performance.ServiceStatus;
import org.engine.process.performance.utils.InnerService;

import akka.japi.Pair;

public class EngingPerformanceMultiPeriods extends InnerService {

	private List<SingleCycle> cyclesList;

	private StringBuffer output = new StringBuffer();
	private String kafkaAddress;
	private String schemaRegustryUrl;  
	private String sourceName; 
	private int num_of_cycles;
	private int num_of_updates;
	
	public EngingPerformanceMultiPeriods(String kafkaAddress,
			String schemaRegistryUrl, String schemaRegistryIdentity,String sourceName) {
		
		this.kafkaAddress = kafkaAddress; 
		this.sourceName = sourceName;
		this.schemaRegustryUrl = schemaRegistryUrl;
		System.out.println("NUM_OF_CYCLES::::::::" + System.getenv("NUM_OF_CYCLES")); 
		System.out.println("NUM_OF_UPDATES::::::::" + System.getenv("NUM_OF_UPDATES")); 
		
		num_of_cycles = Integer.parseInt(System.getenv("NUM_OF_CYCLES"));
		num_of_updates = Integer.parseInt(System.getenv("NUM_OF_UPDATES")); 
		cyclesList = new ArrayList<>();
	}

	@Override
	protected void preExecute() throws Exception {

	}

	@Override
	protected void postExecute() throws Exception {
		
		System.out.println("===postExecute");
		int i = 0;
		for(SingleCycle period : cyclesList ) {
			System.out.println("Cycle "+i);
			int j = 0;
			for( MessageData messageData : period.getMessageDataList()) {
				
				System.out.println("Update "+j);				
				Pair<Long,Long> diffTime = messageData.getHandlePerformanceMessages().getTimeDifferences();
				messageData.setRowDataToSourceDiffTime(diffTime.second());
				messageData.setSourceToUpdateDiffTime(diffTime.first());
				messageData.setLastOffsetForRawData(messageData.getHandlePerformanceMessages().getLastOffsetForRawData());
				messageData.setLastOffsetForSource(messageData.getHandlePerformanceMessages().getLastOffsetForSource());
				messageData.setLastOffsetForUpdate(messageData.getHandlePerformanceMessages().getLastOffsetForUpdate());
				j++;
			} 
			i++;
		}
	}

	@Override
	protected ServiceStatus execute() throws Exception {

		for( int i = 0; i < num_of_cycles; i++ ) {
			
			System.out.println("===>CYCLE " + i); 

			String externalSystemID = utils.randomExternalSystemID();
			SingleCycle singlePeriod = new SingleCycle(); 
			double lat = 4.3;
			double longX = 6.4;

			for(int j = 0 ; j < num_of_updates; j++) {
				System.out.println("UPDATE " + j);
				
				Date startTime = new Date(System.currentTimeMillis());
				String latStr = Double.toString(lat);
				String longXStr = Double.toString(longX);

				HandlePerformanceMessages handlePerformanceMessages = new HandlePerformanceMessages(kafkaAddress,schemaRegustryUrl,
										sourceName,externalSystemID,latStr,longXStr);					
				
				handlePerformanceMessages.handleMessage(); 
				MessageData messageData = new MessageData(startTime,externalSystemID,latStr, longXStr, sourceName,handlePerformanceMessages);
				messageData.setNumOfCycle(i);
				messageData.setNumOfUpdate(j);
				singlePeriod.addMessageData(messageData);
				lat++;
				longX++; 
				Thread.sleep(5000);

			}
			
			cyclesList.add(singlePeriod); 		
		}
		
		System.out.println("END execute  " + cyclesList ); 
		return ServiceStatus.SUCCESS;

	}

	@Override
	public String getOutput() { 
	
		for(SingleCycle period : cyclesList ) {
			
			for( MessageData messageData : period.getMessageDataList()) {
				
				output.append(messageData.toString());
			} 
		}
		
		return output.toString();
	} 
}
