package org.engine.process.performance.activity.create;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException; 
import java.util.ArrayList;
import java.util.Arrays; 
import java.util.Collections;
import java.util.Date;
import java.util.List; 
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors; 

import org.apache.log4j.Logger; 
import org.engine.process.performance.csv.CsvFileWriter;
import org.engine.process.performance.csv.TopicTimeData;
import org.engine.process.performance.utils.InnerService;  
import org.engine.process.performance.utils.Pair;
import org.engine.process.performance.utils.ServiceStatus;
import org.engine.process.performance.utils.SingleCycle;
import org.engine.process.performance.utils.SingleMessageData;
import org.engine.process.performance.utils.MessageConsumerThread;
import org.engine.process.performance.utils.Utils;

public class CreateActivityMultiMessages extends InnerService {

	private TopicTimeData<double[]> createAndUpdateArray;
	private TopicTimeData<double[]> createArray;
	private TopicTimeData<double[]> updateArray; 
	private String seperator = ",";
	private String emptyString = "";	
	final static public Logger logger = Logger.getLogger(CreateActivityMultiMessages.class);
	static {
		Utils.setDebugLevel(logger);
	}
	
	public CreateActivityMultiMessages() {
		super(); 
		cyclesList = Collections.synchronizedList(new ArrayList<SingleCycle>());	 
	}

	@Override
	protected void preExecute() throws Exception {

	}

	@Override
	protected void postExecute() throws Exception {

		logger.debug("===postExecute");

		createAndUpdateArray = new TopicTimeData<double[]>(new double[numOfCycles*numOfUpdatesPerCycle],new double[numOfCycles*numOfUpdatesPerCycle]);
		createArray = new TopicTimeData<double[]>(new double[numOfCycles],new double[numOfCycles]);
		updateArray = new TopicTimeData<double[]>(new double[numOfCycles*(numOfUpdatesPerCycle-1)],new double[numOfCycles*(numOfUpdatesPerCycle-1)]);

		int i = 0;
		int index = 0;
		int index1 = 0;
		int index2 = 0;
		for(SingleCycle cycle : cyclesList ) {
			logger.debug("Cycle "+i);
			int j = 0;
			for( SingleMessageData messageData : cycle.getMessageDataList()) {
				
				CreateMessageData createMessageData = (CreateMessageData) messageData;

				logger.debug("\nUpdate "+j);
				CreateActivityConsumer createActivityConsumer = (CreateActivityConsumer) createMessageData.getActivityConsumer();
				Pair<Long,Long> diffTime = createActivityConsumer.getTimeDifferences();
				createMessageData.setRawDataToSourceDiffTime(diffTime.getRight());
				createMessageData.setSourceToUpdateDiffTime(diffTime.getLeft());
				createAndUpdateArray.getRawDataToSource()[index] = (double) diffTime.getRight();
				createAndUpdateArray.getSourceToUpdate()[index] = (double)diffTime.getLeft(); 

				if( j == 0 ) {
					createArray.getRawDataToSource()[index1] = (double) diffTime.getRight();
					createArray.getSourceToUpdate()[index1] = (double)diffTime.getLeft();
					index1++;
				}
				else {
					
					updateArray.getRawDataToSource()[index2] = (double) diffTime.getRight();
					updateArray.getSourceToUpdate()[index2] = (double) diffTime.getLeft();
					index2++;
				}

				createMessageData.setLastOffsetForRawData(createActivityConsumer.getLastOffsetForRawData());
				createMessageData.setLastOffsetForSource(createActivityConsumer.getLastOffsetForSource());
				createMessageData.setLastOffsetForUpdate(createActivityConsumer.getLastOffsetForUpdate());
				j++;
				index++;
			} 
			i++;
		} 
	}

	@Override
	protected ServiceStatus execute() throws Exception {

		if( durationInMin > 0 && numOfCycles > 0 ) {

			logger.error("Error: Both DURATION and NUM_OF_CYCLE have value"); 
			return ServiceStatus.FAILURE;			
		}

		ExecutorService executor = Executors.newCachedThreadPool();
		if( numOfCycles > 0 ) {

			for( int i = 0; i < numOfCycles; i++ ) {

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
			numOfCycles = 0;
			while ( System.currentTimeMillis() < endTime) {

				logger.debug("===>CYCLE " + numOfCycles);
				SingleCycle singleCycle = runSingleCycle(numOfCycles);
				cyclesList.add(singleCycle); 
				Runnable worker = new MessageConsumerThread(singleCycle);
				executor.execute(worker);
				numOfCycles++;				 
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

		StringBuffer output = new StringBuffer();		
		for(SingleCycle period : cyclesList ) {

			for( SingleMessageData messageData : period.getMessageDataList()) { 
				
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
		output.append("NUM_OF_UPDATES").append(seperator).append(numOfUpdatesPerSec).append(endl);
		output.append("CREATE").append(endl);
		output.append(createCsvFileDataInRows(createArray,sourceName)).append(endl);
		output.append("UPDATE").append(endl);
		output.append(createCsvFileDataInRows(updateArray,sourceName)).append(endl).append(endl);
 
		return output.toString();
	} 

	private SingleCycle runSingleCycle(int numOfCycle) throws IOException, RestClientException, InterruptedException {

		String externalSystemID = utils.randomExternalSystemID();
		SingleCycle singleCycle = new SingleCycle(); 

		for(int j = 0 ; j < numOfUpdatesPerCycle; j++) {
			logger.debug("UPDATE " + j);

			Date startTime = new Date(System.currentTimeMillis()); 
			String identifierId = utils.randomExternalSystemID();
			CreateActivityConsumer createActivityConsumer = new CreateActivityConsumer(sourceName,externalSystemID,identifierId);					

			createActivityConsumer.handleMessage(); 
			CreateMessageData messageData = new CreateMessageData(startTime,externalSystemID,identifierId, sourceName,createActivityConsumer);
			messageData.setNumOfCycle(numOfCycle);
			messageData.setNumOfUpdate(j);
			singleCycle.addMessageData(messageData);  
			Thread.sleep(interval);
		}
		return singleCycle;
	}

	@Override
	public void printOutputToFile(String fileLocation) {

		List<String> header = new ArrayList<String>();
		header.add("NUM_OF_INTERCAES :"+numOfInteraces);
		header.add("NUM_OF_UPDATES :"+numOfUpdatesPerSec); 

		if( durationInMin > 0 ) { 
			header.add("DURATION(MIN) :"+durationInMin);
		}
		else { 
			header.add("NUM_OF_CYCLES :"+numOfCycles);
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
				create.setRawDataToSource(utils.convertToString(createArray.getRawDataToSource()[i]));
			}

			if( i < createArray.getSourceToUpdate().length ) {
				create.setSourceToUpdate(utils.convertToString(createArray.getSourceToUpdate()[i])); 
			} 

			if( i < updateArray.getRawDataToSource().length ) {
				update.setRawDataToSource(utils.convertToString(updateArray.getRawDataToSource()[i]));
			} 

			if( i < updateArray.getSourceToUpdate().length ) {
				update.setSourceToUpdate(utils.convertToString(updateArray.getSourceToUpdate()[i])); 
			} 

			CsvRecordForCreate record = new CsvRecordForCreate(create,update);
			data.add(record); 
		} 

		return data; 
	}

	private String createCsvFileDataInRows(TopicTimeData<double[]> topicTimeData,String sourceName) {

		StringBuffer output = new StringBuffer(); 
		output.append("DiffTime between between <"+sourceName+"-raw-data> and <"+sourceName+">").append(getLine(topicTimeData.getRawDataToSource())).append("\n");
		output.append("DiffTime between <"+sourceName+"> and <update>").append(getLine(topicTimeData.getSourceToUpdate())).append("\n");
		
		return output.toString();
	}	 
} 
