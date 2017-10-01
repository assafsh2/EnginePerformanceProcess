package org.engine.process.performance.activity.merge;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.engine.process.performance.Main;
import org.engine.process.performance.csv.CsvFileWriter;
import org.engine.process.performance.csv.CsvRecordForMerge;
import org.engine.process.performance.utils.InnerService;
import org.engine.process.performance.utils.MessageConsumerThread;
import org.engine.process.performance.utils.Pair;
import org.engine.process.performance.utils.ServiceStatus;
import org.engine.process.performance.utils.SingleCycle;
import org.engine.process.performance.utils.SingleMessageData;
import org.engine.process.performance.utils.Utils;
import org.z.entities.schema.MergeEvent;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.function.Procedure;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

public class MergeActivityMultiMessages extends InnerService {

	private KafkaConsumer<Object, Object> updateConsumer;
	private KafkaConsumer<Object, Object> mergeConsumer;
	private TopicPartition updatePartition;
	private TopicPartition mergePartition;
	private double[] diffTimeArray;
	private static boolean testing = Main.testing;
	final static public Logger logger = Logger.getLogger(MergeActivityMultiMessages.class);
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
	}

	@Override
	protected void postExecute() throws Exception {
		logger.debug("===postExecute");
		diffTimeArray = new double[numOfCycles * numOfUpdatesPerCycle];
		int i = 0;
		int j = 0;
		for (SingleCycle cycle : cyclesList) {
			logger.debug("Cycle " + i);

			for (SingleMessageData messageData : cycle.getMessageDataList()) {

				MergeMessageData mergeMessageData = (MergeMessageData) messageData;
				diffTimeArray[j] = ((MergeActivityConsumer) mergeMessageData
						.getActivityConsumer()).getTimeDiff();
				j++;
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
			}
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		logger.debug("Finished all threads");

		logger.debug("END execute");
		return ServiceStatus.SUCCESS;
	}

	private SingleCycle runSingleCycle(int numOfCycle) throws Exception {

		updateConsumer.seekToEnd(Arrays.asList(updatePartition));
		long lastOffsetForUpdate = updateConsumer.position(updatePartition);
		long newOffset = lastOffsetForUpdate - 1000 > 0 ? lastOffsetForUpdate - 1000
				: 0;
		updateConsumer.seek(updatePartition, newOffset);

		Map<UUID, GenericRecord> entitiesMap = getEntitiesToMerge();

		if (entitiesMap.size() != numOfUpdatesPerCycle * 2) {
			logger.error("Not found entities to merge");
			throw new Exception("Not found entities to merge");
		}
		SingleCycle singleCycle = new SingleCycle();

		List<UUID> uuidList = new ArrayList<>(entitiesMap.keySet());
		for (int i = 0; i < numOfUpdatesPerCycle; i++) {

			MergeMessageData mergeMessageData = sendMergeMessage(
					uuidList.get(i * 2), uuidList.get((i * 2) + 1), entitiesMap);
			((MergeActivityConsumer) mergeMessageData.getActivityConsumer())
					.setLastOffsetForUpdate(lastOffsetForUpdate);
			mergeMessageData.setNumOfCycle(numOfCycle);
			mergeMessageData.setNumOfUpdate(i);

			singleCycle.addMessageData(mergeMessageData);
		}
		return singleCycle;
	}

	private MergeMessageData sendMergeMessage(UUID uuid1, UUID uuid2,
			Map<UUID, GenericRecord> entitiesMap) {

		MergeMessageData mergeMessageData = new MergeMessageData();
		String randomExternalSystemID = utils.randomExternalSystemID();
		MergeEvent mergeEvent = MergeEvent
				.newBuilder()
				.setMergedEntitiesId(
						Arrays.asList(uuid1.toString(), uuid2.toString()))
				.setMetadata(randomExternalSystemID).build();

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

		MergeActivityConsumer mergeActivityConsumer = new MergeActivityConsumer();
		mergeActivityConsumer.setLastOffsetForMerge(lastOffsetForMerge);

		Set<Pair<String, String>> sonsList = new HashSet<>();
		sonsList.addAll(utils.getSonsFromRecrod(entitiesMap.get(uuid1)));
		sonsList.addAll(utils.getSonsFromRecrod(entitiesMap.get(uuid2)));
		mergeActivityConsumer.setSonsList(sonsList);
		mergeActivityConsumer.setMetadata(randomExternalSystemID);
		String[] uuidList = { uuid1.toString(), uuid2.toString() };
		mergeActivityConsumer.setUuidList(uuidList);

		mergeMessageData.setActivityConsumer(mergeActivityConsumer);

		return mergeMessageData;
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
		if (diffTimeArray.length > 1) {
			output.append(
					"The average between <merge> and <update> is "
							+ utils.mean(diffTimeArray)).append(endl);
			output.append(
					"The median between <merge> and <update> is "
							+ utils.median(diffTimeArray)).append(endl);
			output.append(
					"The standard deviation  between <merge> and <update> is "
							+ utils.standardDeviation(diffTimeArray)).append(
					endl);
		}
		output.append("Export to CSV ").append(endl);
		output.append("NUM_OF_INTERCAES").append(seperator)
				.append(numOfInteraces).append(endl);
		output.append("NUM_OF_UPDATES").append(seperator)
				.append(numOfUpdatesPerSec).append(endl);
		output.append("MERGE").append(getLine(diffTimeArray)).append(endl);

		return output.toString();
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
		header.add("NUM_OF_UPDATES_PER_CYCLE :" + numOfUpdatesPerCycle);
		header.add("INTERVAL :" + interval);

		Object[] columnsName = { "DiffTime between <merge> and <update>" };
		List<CsvRecordForMerge> data = new ArrayList<>();
		Arrays.stream(diffTimeArray).forEach(
				m -> data.add(new CsvRecordForMerge(utils.convertToString(m))));

		CsvFileWriter csvFileWriter = new CsvFileWriter(fileLocation);
		csvFileWriter.writeCsvFile(header, columnsName, data);
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

	private Map<UUID, GenericRecord> getEntitiesToMerge() {

		Map<UUID, GenericRecord> entitiesMap = new HashMap<>();
		if (testing) {
			List<Pair<GenericRecord, Long>> records = callConsumersWithAkka("update");
			for (Pair<GenericRecord, Long> pair : records) {

				UUID uuid = UUID.fromString((String) pair.getLeft().get(
						"entityID"));
				entitiesMap.put(uuid, pair.getLeft());
				if (entitiesMap.size() == numOfUpdatesPerCycle * 2)
					break;
			}
		} else {
			ConsumerRecords<Object, Object> records = updateConsumer.poll(1000);
			for (ConsumerRecord<Object, Object> record : records) {

				GenericRecord family = (GenericRecord) record.value();
				UUID uuid = UUID.fromString((String) family.get("entityID")
						.toString());
				entitiesMap.put(uuid, family);
				if (entitiesMap.size() == numOfUpdatesPerCycle * 2)
					break;
			}
		}
		return entitiesMap;
	}

	/**
	 * Only for testing
	 */

	static SchemaRegistryClient schemaRegistry;
	static ActorMaterializer materializer;
	static ActorSystem system;

	public void setTesting(SchemaRegistryClient schemaRegistry,
			ActorMaterializer materializer, ActorSystem system) {
		this.schemaRegistry = schemaRegistry;
		this.materializer = materializer;
		this.system = system;
	}

	public static List<Pair<GenericRecord, Long>> callConsumersWithAkka(
			String topicName) {

		KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer(
				schemaRegistry);
		keyDeserializer.configure(Collections.singletonMap(
				"schema.registry.url", "http://fake-url"), true);

		final ConsumerSettings<String, Object> consumerSettings = ConsumerSettings
				.create(system, new StringDeserializer(), keyDeserializer)
				.withBootstrapServers("192.168.0.51:9092")
				.withGroupId("group1")
				.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
						"earliest");

		List<Pair<GenericRecord, Long>> consumerRecords = new ArrayList<>();

		Procedure<ConsumerRecord<String, Object>> f = new Procedure<ConsumerRecord<String, Object>>() {

			private static final long serialVersionUID = 1L;

			public void apply(ConsumerRecord<String, Object> param)
					throws Exception {

				System.out.println("Param " + param.value());
				consumerRecords.add(new Pair<GenericRecord, Long>(
						(GenericRecord) param.value(), param.timestamp()));
			}
		};

		System.out.println("=====" + topicName);
		CompletionStage<Done> d = Consumer
				.committableSource(consumerSettings,
						Subscriptions.topics(topicName))
				.map(result -> result.record()).runForeach(f, materializer);
		// Wait till the array will be populated from kafka
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
		return consumerRecords;
	}

	private void callProducerWithAkka(String topic, GenericRecord record) {

		ProducerSettings<String, Object> producerSettings = ProducerSettings
				.create(system, new StringSerializer(),
						new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers(kafkaAddress);

		Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer
				.plainSink(producerSettings);

		ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>(
				topic, record);

		Source.from(Arrays.asList(producerRecord)).to(sink).run(materializer);
	}
}
