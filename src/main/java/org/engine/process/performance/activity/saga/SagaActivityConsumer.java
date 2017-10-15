package org.engine.process.performance.activity.saga;

import java.util.ArrayList;
import java.util.Arrays; 
import java.util.HashSet;
import java.util.List; 
import java.util.Set;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.engine.process.performance.Main;
import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.Constant;
import org.engine.process.performance.utils.Pair;
import org.engine.process.performance.utils.Utils;

public class SagaActivityConsumer extends ActivityConsumer {

	private long lastOffsetForMerge;
	private long lastOffsetForUpdate;
	private long lastOffsetForSplit; 
	private KafkaConsumer<Object, Object> mergeConsumer;
	private KafkaConsumer<Object, Object> splitConsumer;
	private KafkaConsumer<Object, Object> updateConsumer;
	private TopicPartition partitionMerge;
	private TopicPartition partitionSplit;
	private TopicPartition partitionUpdate;
	private long mergeTimeStamp;
	private long updateForMergeTimeStamp;
	private long splitTimeStamp;
	private long updateForSplitTimeStamp;
	private String identifierId;
	private static boolean testing = Main.testing;
	final static public Logger logger = Logger
			.getLogger(SagaActivityConsumer.class);
	static {
		Utils.setDebugLevel(logger);
	}

	public SagaActivityConsumer() {
		mergeConsumer = new KafkaConsumer<Object, Object>(utils.getProperties(true));
		partitionMerge = new TopicPartition("merge", 0);
		mergeConsumer.assign(Arrays.asList(partitionMerge)); 

		splitConsumer = new KafkaConsumer<Object, Object>(utils.getProperties(true));
		partitionSplit = new TopicPartition("split", 0);
		splitConsumer.assign(Arrays.asList(partitionSplit)); 

		updateConsumer = new KafkaConsumer<Object, Object>(utils.getProperties(true));
		partitionUpdate = new TopicPartition("update", 0);
		updateConsumer.assign(Arrays.asList(partitionUpdate)); 
	}

	@Override
	public void callConsumer() {		
		mergeConsumer.seek(partitionMerge, lastOffsetForMerge);
		mergeTimeStamp = callTopic("merge",Constant.MERGE_IDENTIFIER_TYPE,mergeConsumer);

		splitConsumer.seek(partitionSplit, lastOffsetForSplit);
		splitTimeStamp = callTopic("split",Constant.SPLIT_IDENTIFIER_TYPE,splitConsumer); 
	} 

	public void setLastOffsetForMerge(long lastOffsetForMerge) {
		this.lastOffsetForMerge = lastOffsetForMerge;
	}

	public void setLastOffsetForUpdate(long lastOffsetForUpdate) {
		this.lastOffsetForUpdate = lastOffsetForUpdate;
	}	

	public void setLastOffsetForSplit(long lastOffsetForSplit) {
		this.lastOffsetForSplit = lastOffsetForSplit;
	} 
	
	public String getIdentifierId() {
		return identifierId;
	}
	
	public void setIdentifierId(String identifierId) {
		this.identifierId = identifierId;
	}

	public long getMergeTimeStamp() {
		return mergeTimeStamp;
	}
	
	public Pair<Long,Long> getTimeDiff() {
		return new Pair<Long,Long>(updateForMergeTimeStamp - mergeTimeStamp, updateForSplitTimeStamp - splitTimeStamp);
	}  

	public Set<UUID> callUpdateTopic(String identifierName) {		
		Set<UUID> entitiesList = new HashSet<>();
		boolean isToContinue = true;
		updateConsumer.seek(partitionUpdate, lastOffsetForUpdate);
		while (isToContinue) {
			if (testing) {
				List<Pair<GenericRecord, Long>> records = SagaActivityMultiMessages.callConsumersWithAkka("update");
				for (Pair<GenericRecord, Long> pair : records) {
					if(isRecordMatched(identifierName, entitiesList, pair.getLeft(), pair.getRight())) {
						isToContinue = false;						
						break;
					}
				}
			} 
			else { 
				ConsumerRecords<Object, Object> records = updateConsumer.poll(10000);
				for (ConsumerRecord<Object, Object> param : records) {
					GenericRecord event = (GenericRecord) param.value();
					if (isRecordMatched(identifierName, entitiesList, event, param.timestamp())) {
						isToContinue = false;
						break;
					}
				}
			}
		}
		return entitiesList;
	}

	private boolean isRecordMatched(String identifierName,
							Set<UUID> entitiesList, GenericRecord record, long timestamp) {
		if(record.get("metadata") == null) { 
			return false;
		}		
		String metadata = (String) record.get("metadata").toString();
		String uuid = (String) record.get("entityID").toString();
		if (metadata.contains(identifierName+"="+identifierId)) {
			logger.debug("Found ConsumerRecord for update : " + record);
			logger.debug("Timetamp : " + timestamp);
			entitiesList.add(UUID.fromString(uuid));			
			if(identifierName.startsWith("MERGE")) {
				updateForMergeTimeStamp = timestamp;
				return true;
			}
			else if(identifierName.startsWith("SPLIT") && entitiesList.size() == 2) {
				updateForSplitTimeStamp = timestamp;
				return true;
			} 
		}
		return false;
	}

	
	private long callTopic(String topic,String identifierName, KafkaConsumer<Object, Object> consumer) {
		while (true) {
			if (testing) {
				List<Pair<GenericRecord, Long>> records = SagaActivityMultiMessages.callConsumersWithAkka(topic);
				for (Pair<GenericRecord, Long> pair : records) {
					String metadata = (String) pair.getLeft().get("metadata").toString();
					if (metadata.contains(identifierName+"="+identifierId)) {
						logger.debug("Found ConsumerRecord for "+topic+" : " + pair.getLeft());
						logger.debug("timestamp : " + pair.getRight());
						return pair.getRight();
					}
				}
			}
			else {
				ConsumerRecords<Object, Object> records = consumer.poll(10000);
				for (ConsumerRecord<Object, Object> param : records) {
					GenericRecord record = (GenericRecord) param.value();
					String metadata = (String) record.get("metadata").toString();
					if (metadata.contains(identifierName+"="+identifierId)) {
						logger.debug("Found ConsumerRecord for "+topic+" : " + param);
						logger.debug("timestamp : " + param.timestamp());
						return param.timestamp();
					}
				}
			}
		}
	}
	

	
}
