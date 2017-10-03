package org.engine.process.performance.activity.saga; 

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.ArrayList;
import java.util.Arrays; 
import java.util.HashMap; 
import java.util.List;
import java.util.Map; 
import java.util.UUID;

import org.apache.avro.generic.GenericRecord; 
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition; 
import org.apache.log4j.Logger;
import org.engine.process.performance.Main;
import org.engine.process.performance.csv.CsvFileWriter;
import org.engine.process.performance.utils.Constant;
import org.engine.process.performance.utils.InnerService;
import org.engine.process.performance.utils.MessageConsumerThread;
import org.engine.process.performance.utils.Pair;
import org.engine.process.performance.utils.ServiceStatus;
import org.engine.process.performance.utils.SingleCycle;
import org.engine.process.performance.utils.SingleMessageData;
import org.engine.process.performance.utils.Utils;
import org.z.entities.schema.MergeEvent;
import org.z.entities.schema.SplitEvent;

public class SagaActivityMultiMessages extends InnerService {

	private KafkaConsumer<Object, Object> updateConsumer;
	private KafkaConsumer<Object, Object> mergeConsumer;
	private KafkaConsumer<Object, Object> splitConsumer;
	private TopicPartition updatePartition;
	private TopicPartition mergePartition;
	private TopicPartition splitPartition;
	private double[] mergeDiffTimeArray;
	private double[] splitDiffTimeArray;
	private static boolean testing = Main.testing;
	final static public Logger logger = Logger.getLogger(SagaActivityMultiMessages.class);
	static {
		Utils.setDebugLevel(logger);
	}

	@Override
	protected void preExecute() throws Exception {
		updateConsumer = new KafkaConsumer<Object, Object>(getProperties(true));
		updatePartition = new TopicPartition("update", 0);
		updateConsumer.assign(Arrays.asList(updatePartition));

		mergeConsumer = new KafkaConsumer<Object, Object>(getProperties(true));
		mergePartition = new TopicPartition("merge", 0);
		mergeConsumer.assign(Arrays.asList(mergePartition));
		
		splitConsumer = new KafkaConsumer<Object, Object>(getProperties(true));
		splitPartition = new TopicPartition("split", 0);
		splitConsumer.assign(Arrays.asList(splitPartition));
	}

	@Override
	protected void postExecute() throws Exception {
		logger.debug("===postExecute");
		mergeDiffTimeArray = new double[numOfCycles];
		splitDiffTimeArray = new double[numOfCycles];
		int i = 0; 
		for (SingleCycle cycle : cyclesList) {
			logger.debug("Cycle " + i);
			for (SingleMessageData messageData : cycle.getMessageDataList()) {
				SagaMessageData mergeMessageData = (SagaMessageData) messageData;
				Pair<Long,Long> pair = ((SagaActivityConsumer) mergeMessageData.getActivityConsumer()).getTimeDiff();
				mergeDiffTimeArray[i] =  pair.getLeft();
				splitDiffTimeArray[i] =  pair.getRight();
			}
			i++;
		}
	}

	@Override
	protected ServiceStatus execute() throws Exception {

		if (durationInMin > 0 && numOfCycles > 0) {

			logger.debug("Error: Both DURATION and NUM_OF_CYCLE have value");
			return ServiceStatus.FAILURE;
		} 
		ExecutorService executor = Executors.newCachedThreadPool();
		
		if (durationInMin > 0) {

			long startTime = System.currentTimeMillis();
			long endTime = startTime + durationInMin * 60000;
			numOfCycles = 0;
			while (System.currentTimeMillis() < endTime) {

				logger.debug("===>CYCLE " + numOfCycles);
				SingleCycle singleCycle = runSingleCycle(numOfCycles);

				cyclesList.add(singleCycle); 
				Runnable worker = new MessageConsumerThread(singleCycle);
				executor.execute(worker);				
				numOfCycles++;
			}
		} else {
			for (int i = 0; i < numOfCycles; i++) {

				logger.debug("===>CYCLE " + i);
				SingleCycle singleCycle = runSingleCycle(i);

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
		for (SingleCycle cycle : cyclesList) {

			for (SingleMessageData messageData : cycle.getMessageDataList()) {

				String msg = messageData.toString();
				if (msg == null)
					return null;
				output.append(messageData.toString());
			}
		}
		output.append(endl);
		if (mergeDiffTimeArray.length > 1) {
			calcStatistic(output,"merge",mergeDiffTimeArray);
			calcStatistic(output,"split",splitDiffTimeArray);
		}
		output.append("Export to CSV ").append(endl);
		output.append("NUM_OF_INTERCAES").append(seperator)
		.append(numOfInteraces).append(endl);
		output.append("NUM_OF_UPDATES").append(seperator)
		.append(numOfUpdatesPerSec).append(endl);
		output.append("MERGE").append(getLine(mergeDiffTimeArray)).append(endl);
		output.append("SPLIT").append(getLine(splitDiffTimeArray)).append(endl);

		return output.toString();
	}

	private void calcStatistic(StringBuffer output, String topic, double[]diffTimeArray) {
		output.append(
				"The average between <"+topic+"> and <update> is "
						+ utils.mean(diffTimeArray)).append(endl);
		output.append(
				"The median between <"+topic+"> and <update> is "
						+ utils.median(diffTimeArray)).append(endl);
		output.append(
				"The standard deviation between <"+topic+"> and <update> is "
						+ utils.standardDeviation(diffTimeArray)).append(endl);		
	}

	@Override
	public void printOutputToFile(String fileLocation) {

		List<String> header = new ArrayList<String>();
		header.add("NUM_OF_INTERCAES :" + numOfInteraces);
		header.add("NUM_OF_UPDATES :" + numOfUpdatesPerSec);

		if (durationInMin > 0) {
			header.add("DURATION(MIN) :" + durationInMin);
		} else {
			header.add("NUM_OF_CYCLES :" + numOfCycles);
		}  
		Object[] columnsName = { "DiffTime between <merge> and <update>", "DiffTime between <split> and <update>"};
		List<CsvRecordForSaga> data = new ArrayList<>();
		for(int i = 0; i < mergeDiffTimeArray.length; i++) {
			data.add(new CsvRecordForSaga(utils.convertToString(mergeDiffTimeArray[i]),utils.convertToString(splitDiffTimeArray[i])));
		}

		CsvFileWriter csvFileWriter = new CsvFileWriter(fileLocation);
		csvFileWriter.writeCsvFile(header, columnsName, data);
	}
	

	/**
	 * Run full cycle
	 * 1.Send merge event
	 * 2.Get results
	 * 3.Send split event
	 * 4.Get results
	 * 
	 * @param numOfCycle
	 * @return
	 * @throws Exception
	 */
	private SingleCycle runSingleCycle(int numOfCycle) throws Exception { 
		SingleCycle singleCycle = new SingleCycle(); 		
		updateConsumer.seekToEnd(Arrays.asList(updatePartition));
		Map<UUID, GenericRecord> entitiesMap = getEntities();
		if (entitiesMap.size() != 2) {
			logger.error("Not found entities to merge");
			throw new Exception("Not found entities to merge");
		}
		 
		long lastOffsetForUpdate = updateConsumer.position(updatePartition);
 
		List<UUID> uuidList = new ArrayList<>(entitiesMap.keySet());		
		SagaMessageData sagaMessageData = sendMergeMessage(uuidList.get(0), uuidList.get(1), entitiesMap);
		
		((SagaActivityConsumer)sagaMessageData.getActivityConsumer()).setLastOffsetForUpdate(lastOffsetForUpdate); 
		
		List<UUID> entityAfterMerge = ((SagaActivityConsumer)sagaMessageData.getActivityConsumer()).callUpdateTopic(Constant.MERGE_IDENTIFIER_TYPE);
	
		sendSplitMessage(entityAfterMerge.get(0), sagaMessageData);		
	//	((SagaActivityConsumer)sagaMessageData.getActivityConsumer()).callUpdateTopic(Constant.SPLIT_IDENTIFIER_TYPE);
		((SagaActivityConsumer) sagaMessageData.getActivityConsumer()).setLastOffsetForUpdate(lastOffsetForUpdate);
		sagaMessageData.setNumOfCycle(numOfCycle); 

		singleCycle.addMessageData(sagaMessageData);
		Thread.sleep(interval);

		return singleCycle;
	}

	private void sendSplitMessage(UUID uuid, SagaMessageData sagaMessageData) {
		SplitEvent splitEvent = SplitEvent
				.newBuilder()
				.setSplittedEntityID(uuid.toString()) 
				.setMetadata(Constant.SPLIT_IDENTIFIER_TYPE+"="+((SagaActivityConsumer) sagaMessageData.getActivityConsumer()).getIdentifierId())
				.build();

		splitConsumer.seekToEnd(Arrays.asList(splitPartition));
		long lastOffsetForSplit = splitConsumer.position(splitPartition);
		((SagaActivityConsumer) sagaMessageData.getActivityConsumer())
				.setLastOffsetForSplit(lastOffsetForSplit);
		logger.debug("Send split event " + splitEvent);

		if (testing) {
			callProducerWithAkka("split", splitEvent);
		} else {
			try (KafkaProducer<Object, Object> producer = new KafkaProducer<>(
					getProperties(true))) {
				ProducerRecord<Object, Object> record = new ProducerRecord<>(
						"split", splitEvent);
				producer.send(record);
			}
		}		
	}

	private SagaMessageData sendMergeMessage(UUID uuid1, UUID uuid2,Map<UUID, GenericRecord> entitiesMap) {
		SagaMessageData sagaMessageData = new SagaMessageData();
		String identifierId = utils.randomExternalSystemID();
		MergeEvent mergeEvent = MergeEvent
				.newBuilder()
				.setMergedEntitiesId(
						Arrays.asList(uuid1.toString(), uuid2.toString()))
						.setMetadata(Constant.MERGE_IDENTIFIER_TYPE+"="+identifierId).build();

		mergeConsumer.seekToEnd(Arrays.asList(mergePartition));
		long lastOffsetForMerge = mergeConsumer.position(mergePartition);
		logger.debug("Send merge event " + mergeEvent);

		if (testing) {
			callProducerWithAkka("merge", mergeEvent);
		} else {
			try (KafkaProducer<Object, Object> producer = new KafkaProducer<>(
					getProperties(true))) {
				ProducerRecord<Object, Object> record = new ProducerRecord<>(
						"merge", mergeEvent);
				producer.send(record);
			}
		}

		SagaActivityConsumer sagaActivityConsumer = new SagaActivityConsumer();
		sagaActivityConsumer.setLastOffsetForMerge(lastOffsetForMerge);  
		sagaActivityConsumer.setIdentifierId(identifierId);

		sagaMessageData.setActivityConsumer(sagaActivityConsumer);

		return sagaMessageData;
	}

	/*
	 * FamilyEntity ProducerRecord(topic=update, partition=null, key=null,
	 * value={
	 * 
	 * "entityID": "73db3baa-6398-4c5b-84d2-a374c9835749", "stateChanges":
	 * "NONE",
	 * 
	 * "entityAttributes": {"basicAttributes": {"coordinate": {"lat":
	 * 32.96493430463744, "long": 34.01703434503374}, "isNotTracked": false,
	 * "entityOffset": 0, "sourceName": "source2"}, "speed": 12.0, "elevation":
	 * 0.0, "course": 0.0, "nationality": "SPAIN", "category": "airplane",
	 * "pictureURL": "url", "height": 0.0, "nickname": "nickname",
	 * "externalSystemID": "24ca978a-90c3-4ab0-b867-ec7eae31a9fc"}, "sons":
	 * 
	 * 
	 * [{"entityID": "73db3baa-6398-4c5b-84d2-a374c9835749", "entityAttributes":
	 * {"basicAttributes": {"coordinate": {"lat": 32.96493430463744, "long":
	 * 34.01703434503374}, "isNotTracked": false, "entityOffset": 0,
	 * "sourceName": "source2"}, "speed": 12.0, "elevation": 0.0, "course": 0.0,
	 * "nationality": "SPAIN", "category": "airplane", "pictureURL": "url",
	 * "height": 0.0, "nickname": "nickname", "externalSystemID":
	 * "24ca978a-90c3-4ab0-b867-ec7eae31a9fc"}}
	 */

	private Map<UUID, GenericRecord> getEntities() { 
		long lastOffsetForUpdate = updateConsumer.position(updatePartition);
		long newOffset = lastOffsetForUpdate - 3 > 0 ? lastOffsetForUpdate - 3 : 0;
		updateConsumer.seek(updatePartition, newOffset);

		Map<UUID, GenericRecord> entitiesMap = new HashMap<>();
		if (testing) {
			List<Pair<GenericRecord, Long>> records = callConsumersWithAkka("update");
			for (Pair<GenericRecord, Long> pair : records) {

				UUID uuid = UUID.fromString((String) pair.getLeft().get(
						"entityID"));
				entitiesMap.put(uuid, pair.getLeft());
				if (entitiesMap.size() == 2)
					break;
			}
		} else {
			ConsumerRecords<Object, Object> records = updateConsumer.poll(1000);
			for (ConsumerRecord<Object, Object> record : records) {

				GenericRecord family = (GenericRecord) record.value();
				UUID uuid = UUID.fromString((String) family.get("entityID")
						.toString());
				entitiesMap.put(uuid, family);
				if (entitiesMap.size() == 2)
					break;
			}
		}
		return entitiesMap;
	}
}
