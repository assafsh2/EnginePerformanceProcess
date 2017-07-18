package org.engine.process.performance.multi;

import java.util.Date;
import java.util.List;

import org.engine.process.performance.ServiceStatus;
import org.engine.process.performance.utils.InnerService;

import akka.japi.Pair;

public class EngingPerformanceMultiPeriods extends InnerService {

	private List<SinglePeriod> periodsList;

	private StringBuffer output = new StringBuffer();
	private String kafkaAddress;
	private String schemaRegustryUrl;  
	private String sourceName; 
	private int num_of_periods;
	private int num_of_updates;
	
	public EngingPerformanceMultiPeriods(String kafkaAddress,
			String schemaRegistryUrl, String schemaRegistryIdentity,String sourceName) {
		
		this.kafkaAddress = kafkaAddress; 
		this.sourceName = sourceName;
		System.out.println("NUM_OF_PERIODS::::::::" + System.getenv("NUM_OF_PERIODS")); 
		System.out.println("AANUM_OF_UPDATES::::::::" + System.getenv("NUM_OF_UPDATES")); 
	System.out.println("GGG"); 
		num_of_periods = Integer.parseInt(System.getenv("NUM_OF_PERIODS"));
		System.out.println("AAA"); 
		num_of_updates = Integer.parseInt(System.getenv("NUM_OF_UPDATES"));
		System.out.println("BBB"); 
	}

	@Override
	protected void preExecute() throws Exception {

	}

	@Override
	protected void postExecute() throws Exception {

		for(SinglePeriod period : periodsList ) {
			
			for( MessageData messageData : period.getMessageDataList()) {
				
				Pair<Long,Long> diffTime = messageData.getHandlePerformanceMessages().getTimeDifferences();
				messageData.setRowDataToSourceDiffTime(diffTime.second());
				messageData.setSourceToUpdateDiffTime(diffTime.first());
			} 
		}
	}

	@Override
	protected ServiceStatus execute() throws Exception {
		System.out.println("CCC"); 
		double lat = 4.3;
		double longX = 6.4;

		for( int i = 0; i < num_of_periods; i++ ) {
			
			System.out.println("PERDIOD " + i); 

			String externalSystemID = utils.randomExternalSystemID();
			SinglePeriod singlePeriod = new SinglePeriod(); 	

			for(int j = 0 ; j < num_of_updates; j++) {
				System.out.println("UPDATE " + j);
				
				Date startTime = new Date(System.currentTimeMillis());
				String latStr = Double.toString(lat);
				String longXStr = Double.toString(longX);

				HandlePerformanceMessages handlePerformanceMessages = new HandlePerformanceMessages(kafkaAddress,schemaRegustryUrl,
										sourceName,externalSystemID,latStr,longXStr);					
				System.out.println("Before  handleMessage " + j);
				handlePerformanceMessages.handleMessage(); 
				singlePeriod.addMessageData(new MessageData(startTime,externalSystemID,latStr, longXStr, sourceName,handlePerformanceMessages));
				System.out.println(singlePeriod.getMessageDataList());
				lat = lat * 1.3;
				longX = longX * 2.3;
				
				Thread.sleep(500);

			}
			
			periodsList.add(singlePeriod); 		
		}
		
		return ServiceStatus.SUCCESS;

	}

	@Override
	public String getOutput() { 
	
		for(SinglePeriod period : periodsList ) {
			
			for( MessageData messageData : period.getMessageDataList()) {
				
				output.append(messageData.toString());
			} 
		}
		
		return output.toString();
	} 
}