package org.engine.process.performance.activity.split;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.engine.process.performance.Main;
import org.engine.process.performance.activity.merge.MergeActivityMultiMessages;
import org.engine.process.performance.utils.InnerService;
import org.engine.process.performance.utils.Pair;
import org.engine.process.performance.utils.ServiceStatus;
import org.engine.process.performance.utils.Utils;

public class SplitActivityMultiMessages extends InnerService {

	private KafkaConsumer<Object, Object> updateConsumer;
	private KafkaConsumer<Object, Object> splitConsumer;
	private TopicPartition updatePartition;
	private TopicPartition splitPartition;
	private double[] diffTimeArray;
	private static boolean testing = Main.testing;
	final static private Logger logger = Logger.getLogger(SplitActivityMultiMessages.class);
	private static final Object sonsList = null;
	static {
		Utils.setDebugLevel(System.getenv("DEBUG_LEVEL"), logger);
	}

	@Override
	protected void preExecute() throws Exception {
		updateConsumer = new KafkaConsumer<Object, Object>(getProperties(true));
		updatePartition = new TopicPartition("update", 0);
		updateConsumer.assign(Arrays.asList(updatePartition));

		splitConsumer = new KafkaConsumer<Object, Object>(getProperties(true));
		splitPartition = new TopicPartition("split", 0);
		splitConsumer.assign(Arrays.asList(splitPartition));
	}

	@Override
	protected void postExecute() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	protected ServiceStatus execute() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getOutput() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void printOutputToFile(String fileLocation) {
		// TODO Auto-generated method stub

	}

	private long callUpdateTopic() {
		while (true) {
			if (testing) {
				List<Pair<GenericRecord, Long>> records = MergeActivityMultiMessages.callConsumersWithAkka("update");
				for (Pair<GenericRecord, Long> pair : records) {
					if (utils.getSonsFromRecrod(pair.getLeft()).equals(sonsList)) {
						logger.debug("Found ConsumerRecord for update : " + pair.getLeft());
						logger.debug("timestamp : " + pair.getRight());
						return pair.getRight();
					}
				}
			}
			else {
				ConsumerRecords<Object, Object> records = updateConsumer.poll(10000);
				for (ConsumerRecord<Object, Object> param : records) {
					GenericRecord family = (GenericRecord) param.value();
					if (utils.getSonsFromRecrod(family).equals(sonsList)) {
						logger.debug("Found ConsumerRecord for update : " + param);
						logger.debug("timestamp : " + param.timestamp());
						return param.timestamp();
					}
				}
			}
		}
	}

}
