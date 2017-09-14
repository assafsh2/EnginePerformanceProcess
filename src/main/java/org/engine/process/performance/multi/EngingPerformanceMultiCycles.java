package org.engine.process.performance.multi;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException; 
import java.util.ArrayList;
import java.util.Arrays; 
import java.util.Collections;
import java.util.Date;
import java.util.List; 
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.engine.process.performance.ServiceStatus;
import org.engine.process.performance.csv.CsvFileWriter;
import org.engine.process.performance.csv.CsvRecordForCreate;
import org.engine.process.performance.csv.TopicTimeData;
import org.engine.process.performance.utils.InnerService; 

import akka.japi.Pair;

public class EngingPerformanceMultiCycles extends InnerService {

	private List<SingleCycle> cyclesList;
	private StringBuffer output = new StringBuffer();
	private String kafkaAddress;
	private String schemaRegustryUrl;  
	private String sourceName; 
	private int num_of_cycles;
	private int num_of_updates_per_cycle;
	private String endl = "\n";  

	private TopicTimeData<double[]> createAndUpdateArray;
	private TopicTimeData<double[]> createArray;
	private TopicTimeData<double[]> updateArray; 
	
	private int interval;
	private int durationInMin; 
	private int numOfInteraces;
	private int numOfUpdates;
	private String seperator = ",";
	private String emptyString = "";
	
	public EngingPerformanceMultiCycles(String kafkaAddress,
			String schemaRegistryUrl, String schemaRegistryIdentity,String sourceName) {

		this.kafkaAddress = kafkaAddress; 
		this.sourceName = sourceName;
		this.schemaRegustryUrl = schemaRegistryUrl;
		cyclesList = Collections.synchronizedList(new ArrayList<SingleCycle>());	

		logger.debug("NUM_OF_CYCLES::::::::" + System.getenv("NUM_OF_CYCLES")); 
		logger.debug("NUM_OF_UPDATES_PER_CYCLE::::::::" + System.getenv("NUM_OF_UPDATES_PER_CYCLE")); 
		logger.debug("DURATION (in Minute)::::::::" + System.getenv("DURATION")); 
		logger.debug("INTERVAL::::::::" + System.getenv("INTERVAL")); 
		logger.debug("NUM_OF_INTERFACES::::::::" + System.getenv("NUM_OF_INTERFACES")); 
		logger.debug("NUM_OF_UPDATES::::::::" + System.getenv("NUM_OF_UPDATES")); 

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

		logger.debug("===postExecute");

		createAndUpdateArray = new TopicTimeData<double[]>(new double[num_of_cycles*num_of_updates_per_cycle],new double[num_of_cycles*num_of_updates_per_cycle]);
		createArray = new TopicTimeData<double[]>(new double[num_of_cycles],new double[num_of_cycles]);
		updateArray = new TopicTimeData<double[]>(new double[num_of_cycles*(num_of_updates_per_cycle-1)],new double[num_of_cycles*(num_of_updates_per_cycle-1)]);

		int i = 0;
		int index = 0;
		int index1 = 0;
		int index2 = 0;
		for(SingleCycle cycle : cyclesList ) {
			logger.debug("Cycle "+i);
			int j = 0;
			for( MessageData messageData : cycle.getMessageDataList()) {

				logger.debug("\nUpdate "+j);				
				Pair<Long,Long> diffTime = messageData.getHandlePerformanceMessages().getTimeDifferences();
				messageData.setRawDataToSourceDiffTime(diffTime.second());
				messageData.setSourceToUpdateDiffTime(diffTime.first());
				createAndUpdateArray.getRawDataToSource()[index] = (double) diffTime.second();
				createAndUpdateArray.getSourceToUpdate()[index] = (double)diffTime.first(); 

				if( j == 0 ) {
					createArray.getRawDataToSource()[index1] = (double) diffTime.second();
					createArray.getSourceToUpdate()[index1] = (double)diffTime.first();
					index1++;
				}
				else {
					
					updateArray.getRawDataToSource()[index2] = (double) diffTime.second();
					updateArray.getSourceToUpdate()[index2] = (double) diffTime.first();
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

			logger.debug("Error: Both DURATION and NUM_OF_CYCLE have value"); 
			return ServiceStatus.FAILURE;			
		}

		ExecutorService executor = Executors.newFixedThreadPool(5);
		if( num_of_cycles > 0 ) {

			for( int i = 0; i < num_of_cycles; i++ ) {

				logger.debug("===>CYCLE " + i); 
				SingleCycle singlePeriod = runSingleCycle(i); 		
				cyclesList.add(singlePeriod);
				Runnable worker = new MessageConsumerThread(singlePeriod);
				executor.execute(worker);
			}
		}
		else {

			long startTime = System.currentTimeMillis();
			long endTime = startTime + durationInMin * 60000;	 
			num_of_cycles = 0;
			while ( System.currentTimeMillis() < endTime) {

				logger.debug("===>CYCLE " + num_of_cycles);
				SingleCycle singlePeriod = runSingleCycle(num_of_cycles);
				cyclesList.add(singlePeriod); 
				Runnable worker = new MessageConsumerThread(singlePeriod);
				executor.execute(worker);
				num_of_cycles++;				 
			} 
		}

		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		logger.debug("Finished all threads");

		logger.debug("END execute"); 
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

		Arrays.sort(createAndUpdateArray.getRawDataToSource());
		Arrays.sort(createAndUpdateArray.getSourceToUpdate());

		output.append(endl);
		output.append("The average between <"+sourceName+"-raw-data> and <"+sourceName+"> is "+utils.mean(createAndUpdateArray.getRawDataToSource()) ).append(endl);
		output.append("The average between <"+sourceName+"> and <update> is "+utils.mean(createAndUpdateArray.getSourceToUpdate())).append(endl);
		output.append("The median between <"+sourceName+"-raw-data> and <"+sourceName+"> is "+utils.median(createAndUpdateArray.getRawDataToSource())).append(endl);
		output.append("The median between <"+sourceName+"> and <update> is "+utils.median(createAndUpdateArray.getSourceToUpdate())).append(endl);
		output.append("The standard deviation between <"+sourceName+"-raw-data> and <"+sourceName+"> is "+utils.standardDeviation(createAndUpdateArray.getRawDataToSource())).append(endl);
		output.append("The standard deviation  between <"+sourceName+"> and <update> is "+utils.standardDeviation(createAndUpdateArray.getSourceToUpdate())).append(endl);

		output.append("Export to CSV ").append(endl);
		output.append("NUM_OF_INTERCAES").append(seperator).append(numOfInteraces).append(endl);
		output.append("NUM_OF_UPDATES").append(seperator).append(numOfUpdates).append(endl);
		output.append("CREATE").append(endl);
		output.append(createCsvFileDataInRows(createArray,sourceName)).append(endl);
		output.append("UPDATE").append(endl);
		output.append(createCsvFileDataInRows(updateArray,sourceName)).append(endl).append(endl);
 
		return output.toString();
	} 

	private SingleCycle runSingleCycle(int numOfCycle) throws IOException, RestClientException, InterruptedException {

		String externalSystemID = utils.randomExternalSystemID();
		SingleCycle singleCycle = new SingleCycle(); 
		double lat = 4.3;
		double longX = 6.4;

		for(int j = 0 ; j < num_of_updates_per_cycle; j++) {
			logger.debug("UPDATE " + j);

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
			Thread.sleep(interval);
		}

		return singleCycle;
	}

	@Override
	public void printOutputToFile(String fileLocation) {

		List<String> header = new ArrayList<String>();
		header.add("NUM_OF_INTERCAES :"+numOfInteraces);
		header.add("NUM_OF_UPDATES :"+numOfUpdates); 

		if( durationInMin > 0 ) { 
			header.add("DURATION(MIN) :"+durationInMin);
		}
		else { 
			header.add("NUM_OF_CYCLES :"+num_of_cycles);
		}
		header.add("INTERVAL :"+interval); 

		Object[] columnsName = {"CREATE - DiffTime between <"+sourceName+"-raw-data> and <"+sourceName+">",
				"CREATE - DiffTime between <"+sourceName+"> and <update>",
				"UPDATE - DiffTime between between <"+sourceName+"-raw-data> and <"+sourceName+">",
				"UPDATE - DiffTime between <"+sourceName+"> and <update>"};

		List<CsvRecordForCreate> data =	getDataInColumns(); 

		CsvFileWriter csvFileWriter = new CsvFileWriter(fileLocation);
		csvFileWriter.writeCsvFile(header, columnsName, data);
	}

	private List<CsvRecordForCreate> getDataInColumns() {
 
		int[] maxArray = new int[]{createArray.getRawDataToSource().length,createArray.getSourceToUpdate().length,
								   updateArray.getRawDataToSource().length,updateArray.getSourceToUpdate().length};

		Arrays.sort(maxArray);
		int max = maxArray[maxArray.length - 1];

		List<CsvRecordForCreate> data = new ArrayList<>();

		for( int i = 0; i < max; i++) {

			TopicTimeData<String> create = new TopicTimeData<String>(emptyString,emptyString);
			TopicTimeData<String> update = new TopicTimeData<String>(emptyString,emptyString);

			if( i < createArray.getRawDataToSource().length ) { 
				create.setRawDataToSource(convertToString(createArray.getRawDataToSource()[i]));
			}

			if( i < createArray.getSourceToUpdate().length ) {
				create.setSourceToUpdate(convertToString(createArray.getSourceToUpdate()[i])); 
			} 

			if( i < updateArray.getRawDataToSource().length ) {
				update.setRawDataToSource(convertToString(updateArray.getRawDataToSource()[i]));
			} 

			if( i < updateArray.getSourceToUpdate().length ) {
				update.setSourceToUpdate(convertToString(updateArray.getSourceToUpdate()[i])); 
			} 

			CsvRecordForCreate record = new CsvRecordForCreate(create,update);
			data.add(record); 
		} 

		return data; 
	}
	
	private String convertToString (double num) {
		
		Long d =  new Double(num).longValue();
		return String.valueOf(d); 
	}
	 
	private String createCsvFileDataInRows(TopicTimeData<double[]> topicTimeData,String sourceName) {

		StringBuffer output = new StringBuffer(); 
		output.append("DiffTime between between <"+sourceName+"-raw-data> and <"+sourceName+">").append(getLine(topicTimeData.getRawDataToSource())).append("\n");
		output.append("DiffTime between <"+sourceName+"> and <update>").append(getLine(topicTimeData.getSourceToUpdate())).append("\n");
		
		return output.toString();
	}	 
	 
	private String getLine(double[] array) {
		
		StringBuffer output = new StringBuffer();
		for(double d : array ) 
		{
			output.append(seperator).append(d);
		}
		
		return output.toString();
		
	}  
} 
