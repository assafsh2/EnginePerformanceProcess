package org.engine.process.performance.activity.split;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.engine.process.performance.Main;
import org.engine.process.performance.activity.merge.MergeActivityConsumer;
import org.engine.process.performance.activity.merge.MergeActivityMultiMessages;
import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.Pair;
import org.engine.process.performance.utils.Utils;

public class SplitActivityConsumer extends ActivityConsumer {

	private long lastOffsetForSplit;
	private long lastOffsetForUpdate;
	private Set<Pair<String, String>> sonsList;
	private String[] uuidList;
	private KafkaConsumer<Object, Object> splitConsumer;
	private KafkaConsumer<Object, Object> updateConsumer;
	private TopicPartition partitionSplit;
	private TopicPartition partitionUpdate;
	private long splitToUpdateTimeDiff;
	private String metadata;
	private static boolean testing = Main.testing;
	final static private Logger logger = Logger.getLogger(SplitActivityConsumer.class);
	static {
		Utils.setDebugLevel(System.getenv("DEBUG_LEVEL"), logger);
	}
 
	@Override
	public void callConsumer() throws Exception {
		splitConsumer = new KafkaConsumer<Object, Object>(utils.getProperties(true));
		partitionSplit = new TopicPartition("split", 0);
		splitConsumer.assign(Arrays.asList(partitionSplit));
		splitConsumer.seek(partitionSplit, lastOffsetForSplit);
		long splitTimestamp = callSplitTopic();

		updateConsumer = new KafkaConsumer<Object, Object>(utils.getProperties(true));
		partitionUpdate = new TopicPartition("update", 0);
		updateConsumer.assign(Arrays.asList(partitionUpdate));
		updateConsumer.seek(partitionUpdate, lastOffsetForUpdate);
		long updateTimestamp = callUpdateTopic();

		splitToUpdateTimeDiff = updateTimestamp - splitTimestamp;
	}

	private long callUpdateTopic() {
		// TODO Auto-generated method stub
		return 0;
	}

	private long callSplitTopic() {
		while (true) { 
			if(testing) {
				List<Pair<GenericRecord,Long>> records = MergeActivityMultiMessages.callConsumersWithAkka("split");
				for(Pair<GenericRecord,Long> pair : records) {

					String metadata = (String) pair.getLeft().get("metadata").toString();
					if(this.metadata.equals(metadata)) {
						logger.debug("Found ConsumerRecord for merge : "+pair.getLeft());
						logger.debug("timestamp : "+pair.getRight());
						return pair.getRight();
					}
				}
			}
			else {
				ConsumerRecords<Object, Object> records = splitConsumer.poll(10000);
				for (ConsumerRecord<Object, Object> param : records) {

					GenericRecord mergeEvent = (GenericRecord)param.value(); 
					String metadata = (String) mergeEvent.get("metadata").toString();
					if(this.metadata.equals(metadata)) {
						logger.debug("Found ConsumerRecord for merge : "+param);
						logger.debug("timestamp : "+param.timestamp());
						return param.timestamp();
					} 
				}
			}
		}		 
	}

}
