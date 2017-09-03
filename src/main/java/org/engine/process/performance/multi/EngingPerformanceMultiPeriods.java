package org.engine.process.performance.multi;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	private int num_of_updates_per_cycle;
	private String endl = "\n";
	private double[] rowDataToSourceDiffTimeArray;
	private double[] sourceToUpdateDiffTimeArray;
	private double[] totalDiffTimeArray;
	
	private double[] rowDataToSourceDiffTimeUpdateArray;
	private double[] sourceToUpdateDiffTimeUpdateArray;
	private double[] totalDiffTimeUpdateArray;
	private double[] rowDataToSourceDiffTimeCreateArray;
	private double[] sourceToUpdateDiffTimeCreateArray;
	private double[] totalDiffTimeCreateArray;
	private int interval;
	private int durationInMin; 
	private int numOfInteraces;
	private int numOfUpdates;
	private String seperator = ",";
	
	public EngingPerformanceMultiPeriods(String kafkaAddress,
			String schemaRegistryUrl, String schemaRegistryIdentity,String sourceName) {
		
		this.kafkaAddress = kafkaAddress; 
		this.sourceName = sourceName;
		this.schemaRegustryUrl = schemaRegistryUrl;
		cyclesList = Collections.synchronizedList(new ArrayList<SingleCycle>());	
		
		System.out.println("NUM_OF_CYCLES::::::::" + System.getenv("NUM_OF_CYCLES")); 
		System.out.println("NUM_OF_UPDATES_PER_CYCLE::::::::" + System.getenv("NUM_OF_UPDATES_PER_CYCLE")); 
		System.out.println("DURATION (in Minute)::::::::" + System.getenv("DURATION")); 
		System.out.println("INTERVAL::::::::" + System.getenv("INTERVAL")); 
		System.out.println("NUM_OF_INTERFACES::::::::" + System.getenv("NUM_OF_INTERFACES")); 
		System.out.println("NUM_OF_UPDATES::::::::" + System.getenv("NUM_OF_UPDATES")); 
		
		num_of_cycles = Integer.parseInt(System.getenv("NUM_OF_CYCLES"));
		num_of_updates_per_cycle = Integer.parseInt(System.getenv("NUM_OF_UPDATES_PER_CYCLE")); 
		interval = Integer.parseInt(System.getenv("INTERVAL"));
		durationInMin = Integer.parseInt(System.getenv("DURATION"));	
		numOfInteraces = Integer.parseInt(System.getenv("NUM_OF_INTERFACES"));
		numOfUpdates = Integer.parseInt(System.getenv("NUM_OF_UPDATES")); 
	}

	@Override
	protected void preExecute() throws Exception {

	}

	@Override
	protected void postExecute() throws Exception {
		
		System.out.println("===postExecute");
		
		rowDataToSourceDiffTimeArray = new double[num_of_cycles*num_of_updates_per_cycle];
		sourceToUpdateDiffTimeArray= new double[num_of_cycles*num_of_updates_per_cycle];
		totalDiffTimeArray= new double[num_of_cycles*num_of_updates_per_cycle];
		
		rowDataToSourceDiffTimeCreateArray = new double[num_of_cycles];
		sourceToUpdateDiffTimeCreateArray= new double[num_of_cycles];
		totalDiffTimeCreateArray= new double[num_of_cycles];
		
		rowDataToSourceDiffTimeUpdateArray = new double[num_of_cycles*(num_of_updates_per_cycle-1)];
		sourceToUpdateDiffTimeUpdateArray = new double[num_of_cycles*(num_of_updates_per_cycle-1)];
		totalDiffTimeUpdateArray = new double[num_of_cycles*(num_of_updates_per_cycle-1)]; 
		
		int i = 0;
		int index = 0;
		int index1 = 0;
		int index2 = 0;
		for(SingleCycle cycle : cyclesList ) {
			System.out.println("Cycle "+i);
			int j = 0;
			for( MessageData messageData : cycle.getMessageDataList()) {
				
				System.out.println("\nUpdate "+j);				
				Pair<Long,Long> diffTime = messageData.getHandlePerformanceMessages().getTimeDifferences();
				messageData.setRowDataToSourceDiffTime(diffTime.second());
				messageData.setSourceToUpdateDiffTime(diffTime.first());
				rowDataToSourceDiffTimeArray[index] = (double) diffTime.second();
				sourceToUpdateDiffTimeArray[index] = (double)diffTime.first();
				totalDiffTimeArray[index] = (double) diffTime.first()+diffTime.second();
				
				if( j == 0 ) {
					rowDataToSourceDiffTimeCreateArray[index1] = (double) diffTime.second();
					sourceToUpdateDiffTimeCreateArray[index1] = (double)diffTime.first();
					totalDiffTimeCreateArray[index1] = (double) diffTime.first()+diffTime.second();	
					index1++;
				}
				else {
					rowDataToSourceDiffTimeUpdateArray[index2] = (double) diffTime.second();
					sourceToUpdateDiffTimeUpdateArray[index2] = (double)diffTime.first();
					totalDiffTimeUpdateArray[index2] = (double) diffTime.first()+diffTime.second();
					index2++;
				}
				
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

		if( durationInMin > 0 && num_of_cycles > 0 ) {

			System.out.println("Error: Both DURATION and NUM_OF_CYCLE have value"); 
			return ServiceStatus.FAILURE;			
		}

		if( num_of_cycles > 0 ) {

			for( int i = 0; i < num_of_cycles; i++ ) {

				System.out.println("===>CYCLE " + i); 

				runSingleCycle(i); 		
			}
		}
		else {
			
			ExecutorService executor = Executors.newFixedThreadPool(5);
			long startTime = System.currentTimeMillis();
			long endTime = startTime + durationInMin * 60000;	 
			num_of_cycles = 0;
			while ( System.currentTimeMillis() < endTime) {
				
				System.out.println("===>CYCLE " + num_of_cycles);
				SingleCycle singlePeriod = runSingleCycle(num_of_cycles);
				cyclesList.add(singlePeriod); 
				Runnable worker = new MessageConsumerThread(singlePeriod);
				executor.execute(worker);
				num_of_cycles++;
				
				Thread.sleep(interval);
			}
			
	        executor.shutdown();
	        while (!executor.isTerminated()) {
	        }
	        System.out.println("Finished all threads");
		}

		System.out.println("END execute"); 
		return ServiceStatus.SUCCESS;

	}
	
	@Override
	public String getOutput() { 
	
		for(SingleCycle period : cyclesList ) {
			
			for( MessageData messageData : period.getMessageDataList()) {
				
				String msg = messageData.toString();
				if(msg == null) 
					return null;
				output.append(messageData.toString());
			} 
		}
		
		//String csvData = utils.createCsvFile(rowDataToSourceDiffTimeArray,sourceToUpdateDiffTimeArray,totalDiffTimeArray,sourceName);
 		
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
		
		output.append("Export to CSV ").append(endl);
		output.append("NUM_OF_INTERCAES").append(seperator).append(numOfInteraces).append(endl);
		output.append("NUM_OF_UPDATES").append(seperator).append(numOfUpdates).append(endl);
		output.append("CREATE").append(endl);
		output.append(utils.createCsvFileDataInRows(rowDataToSourceDiffTimeCreateArray,sourceToUpdateDiffTimeCreateArray,totalDiffTimeCreateArray,sourceName)).append(endl);
		output.append("UPDATE").append(endl);
		output.append(utils.createCsvFileDataInRows(rowDataToSourceDiffTimeUpdateArray,sourceToUpdateDiffTimeUpdateArray,totalDiffTimeUpdateArray,sourceName)).append(endl).append(endl);
			
		output.append(utils.createCsvFileDataInColumns(rowDataToSourceDiffTimeCreateArray,sourceToUpdateDiffTimeCreateArray,totalDiffTimeCreateArray,
							rowDataToSourceDiffTimeUpdateArray,sourceToUpdateDiffTimeUpdateArray,totalDiffTimeUpdateArray,sourceName)).append(endl);
				                       
		
		
		return output.toString();
	} 
	
	private SingleCycle runSingleCycle(int numOfCycle) throws IOException, RestClientException, InterruptedException {
		
		String externalSystemID = utils.randomExternalSystemID();
		SingleCycle singleCycle = new SingleCycle(); 
		double lat = 4.3;
		double longX = 6.4;

		for(int j = 0 ; j < num_of_updates_per_cycle; j++) {
			System.out.println("UPDATE " + j);
			
			Date startTime = new Date(System.currentTimeMillis());
			String latStr = Double.toString(lat);
			String longXStr = Double.toString(longX);

			HandlePerformanceMessages handlePerformanceMessages = new HandlePerformanceMessages(kafkaAddress,schemaRegustryUrl,
									sourceName,externalSystemID,latStr,longXStr);					
			
			handlePerformanceMessages.handleMessage(); 
			MessageData messageData = new MessageData(startTime,externalSystemID,latStr, longXStr, sourceName,handlePerformanceMessages);
			messageData.setNumOfCycle(numOfCycle);
			messageData.setNumOfUpdate(j);
			singleCycle.addMessageData(messageData);
			lat++;
			longX++; 
			Thread.sleep(5);

		}
		
		return singleCycle;
	}

	@Override
	public String getOutputToFile() {
		
		StringBuffer outputToFile = new StringBuffer();
		outputToFile.append("NUM_OF_INTERCAES").append(seperator).append(numOfInteraces).append(endl);
		outputToFile.append("NUM_OF_UPDATES").append(seperator).append(numOfUpdates).append(endl);
		if( durationInMin > 0 ) {
			outputToFile.append("DURATION(MIN)").append(seperator).append(num_of_cycles).append(endl);
		}
		else {
			outputToFile.append("NUM_OF_CYCLES").append(seperator).append(num_of_cycles).append(endl);
		}
		outputToFile.append("INTERVAL").append(interval).append(endl); 
		outputToFile.append("CREATE").append(endl);
		outputToFile.append(utils.createCsvFileDataInRows(rowDataToSourceDiffTimeCreateArray,sourceToUpdateDiffTimeCreateArray,totalDiffTimeCreateArray,sourceName)).append(endl);
		outputToFile.append("UPDATE").append(endl);
		outputToFile.append(utils.createCsvFileDataInRows(rowDataToSourceDiffTimeUpdateArray,sourceToUpdateDiffTimeUpdateArray,totalDiffTimeUpdateArray,sourceName)).append(endl);
			
		return outputToFile.toString();
	 
		 
	}
} 
