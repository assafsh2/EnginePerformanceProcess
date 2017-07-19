package org.engine.process.performance.multi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.engine.process.performance.ServiceStatus;
import org.engine.process.performance.utils.InnerService;
import org.engine.process.performance.utils.StdStats;

import akka.japi.Pair;

public class EngingPerformanceMultiPeriods extends InnerService {

	private List<SingleCycle> cyclesList;

	private StringBuffer output = new StringBuffer();
	private String kafkaAddress;
	private String schemaRegustryUrl;  
	private String sourceName; 
	private int num_of_cycles;
	private int num_of_updates;
	private String endl = "\n";
	private double[] rowDataToSourceDiffTimeArray;
	private double[] sourceToUpdateDiffTimeArray;
	private double[] totalDiffTimeArray;
	
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
		rowDataToSourceDiffTimeArray = new double[num_of_cycles*num_of_updates];
		sourceToUpdateDiffTimeArray= new double[num_of_cycles*num_of_updates];
		totalDiffTimeArray= new double[num_of_cycles*num_of_updates];
	}

	@Override
	protected void preExecute() throws Exception {

	}

	@Override
	protected void postExecute() throws Exception {
		
		System.out.println("===postExecute");
		int i = 0;
		int index = 0;
		for(SingleCycle period : cyclesList ) {
			System.out.println("Cycle "+i);
			int j = 0;
			for( MessageData messageData : period.getMessageDataList()) {
				
				System.out.println("\nUpdate "+j);				
				Pair<Long,Long> diffTime = messageData.getHandlePerformanceMessages().getTimeDifferences();
				messageData.setRowDataToSourceDiffTime(diffTime.second());
				messageData.setSourceToUpdateDiffTime(diffTime.first());
				rowDataToSourceDiffTimeArray[index] = (double) diffTime.second();
				sourceToUpdateDiffTimeArray[index] = (double)diffTime.first();
				totalDiffTimeArray[index] = (double) diffTime.first()+diffTime.second();
				messageData.setLastOffsetForRawData(messageData.getHandlePerformanceMessages().getLastOffsetForRawData());
				messageData.setLastOffsetForSource(messageData.getHandlePerformanceMessages().getLastOffsetForSource());
				messageData.setLastOffsetForUpdate(messageData.getHandlePerformanceMessages().getLastOffsetForUpdate());
				j++;
				index++;
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
 		
		Arrays.sort(rowDataToSourceDiffTimeArray);
		Arrays.sort(sourceToUpdateDiffTimeArray);
		Arrays.sort(totalDiffTimeArray);
		
		
		output.append(endl);
		output.append("The average between <"+sourceName+"-row-data> and <"+sourceName+"> is "+utils.mean(rowDataToSourceDiffTimeArray) ).append(endl);
		output.append("The average between <"+sourceName+"> and <update> is "+utils.mean(sourceToUpdateDiffTimeArray)).append(endl);
		output.append("The average of total is "+utils.mean(totalDiffTimeArray)).append(endl);
		output.append("The median between <"+sourceName+"-row-data> and <"+sourceName+"> is "+utils.median(rowDataToSourceDiffTimeArray)).append(endl);
		output.append("The median between <"+sourceName+"> and <update> is "+utils.median(sourceToUpdateDiffTimeArray)).append(endl);
		output.append("The median of total is "+utils.median(totalDiffTimeArray)).append(endl);
		output.append("The standard deviation between <"+sourceName+"-row-data> and <"+sourceName+"> is "+utils.standardDeviation(rowDataToSourceDiffTimeArray)).append(endl);
		output.append("The standard deviation  between <"+sourceName+"> and <update> is "+utils.standardDeviation(sourceToUpdateDiffTimeArray)).append(endl);
		output.append("The standard deviation  of total is "+utils.standardDeviation(totalDiffTimeArray)).append(endl);
		
		output.append("The standard deviation between <"+sourceName+"-row-data> and <"+sourceName+"> is "+StdStats.stddev(rowDataToSourceDiffTimeArray)).append(endl);
		output.append("The standard deviation  between <"+sourceName+"> and <update> is "+StdStats.stddev(sourceToUpdateDiffTimeArray)).append(endl);
		output.append("The standard deviation  of total is "+StdStats.stddev(totalDiffTimeArray)).append(endl);
 
		
		return output.toString();
	} 
}
