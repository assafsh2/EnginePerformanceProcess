package org.engine.process.performance.activity.merge;

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
import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.Pair;
import org.engine.process.performance.utils.Utils; 

public class MergeActivityConsumer extends ActivityConsumer{

	private long lastOffsetForMerge;
	private long lastOffsetForUpdate;
	private Set<Pair<String,String>> sonsList;
	private String[] uuidList;
	private KafkaConsumer<Object, Object> mergeConsumer;
	private KafkaConsumer<Object, Object> updateConsumer;
	private TopicPartition partitionMerge; 
	private TopicPartition partitionUpdate;
	private long mergeToUpdateTimeDiff;
	private String metadata; 
	private static boolean testing = Main.testing;
	final static public Logger logger = Logger.getLogger(MergeActivityConsumer.class);
	static {
		Utils.setDebugLevel(System.getenv("DEBUG_LEVEL"),logger);
	}


	@Override
	public void callConsumer() {

		mergeConsumer = new KafkaConsumer<Object, Object>(utils.getProperties(true));
		partitionMerge = new TopicPartition("merge",0);
		mergeConsumer.assign(Arrays.asList(partitionMerge));
		mergeConsumer.seek(partitionMerge, lastOffsetForMerge);
		long mergeTimestamp = callMergeTopic();

		updateConsumer = new KafkaConsumer<Object, Object>(utils.getProperties(true));
		partitionUpdate = new TopicPartition("update",0);
		updateConsumer.assign(Arrays.asList(partitionUpdate));
		updateConsumer.seek(partitionUpdate, lastOffsetForUpdate);
		long updateTimestamp = callUpdateTopic();

		mergeToUpdateTimeDiff = updateTimestamp - mergeTimestamp;
	}  

	public void setMergeConsumer(KafkaConsumer<Object, Object> mergeConsumer) {
		this.mergeConsumer = mergeConsumer;
	}

	public void setUpdateConsumer(KafkaConsumer<Object, Object> updateConsumer) {
		this.updateConsumer = updateConsumer;
	}

	public void setPartitionMerge(TopicPartition partitionMerge) {
		this.partitionMerge = partitionMerge;
	}

	public void setPartitionUpdate(TopicPartition partitionUpdate) {
		this.partitionUpdate = partitionUpdate;
	}

	public void setMergeToUpdateTimeDiff(long mergeToUpdateTimeDiff) {
		this.mergeToUpdateTimeDiff = mergeToUpdateTimeDiff;
	}



	public void setLastOffsetForMerge(long lastOffsetForMerge) {
		this.lastOffsetForMerge = lastOffsetForMerge;
	} 

	public void setLastOffsetForUpdate(long lastOffsetForUpdate) {
		this.lastOffsetForUpdate = lastOffsetForUpdate;
	}

	public void setSonsList(Set<Pair<String, String>> sonsList) {
		this.sonsList = sonsList;
	}  

	public void setMetadata(String metadata) {
		this.metadata = metadata;
	}

	public long getTimeDiff() {
		return mergeToUpdateTimeDiff;
	}

	public String[] getUuidList() {
		return uuidList;
	}

	public void setUuidList(String[] uuidList) {
		this.uuidList = uuidList;
	}

	private long callMergeTopic() {

		while (true) { 

			if(testing) {
				List<Pair<GenericRecord,Long>> records = MergeActivityMultiMessages.callConsumersWithAkka("merge");
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
				ConsumerRecords<Object, Object> records = mergeConsumer.poll(10000);
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

	private long callUpdateTopic() {

		while (true) {

			if(testing) {
				List<Pair<GenericRecord,Long>> records = MergeActivityMultiMessages.callConsumersWithAkka("update");
				for(Pair<GenericRecord,Long> pair : records) {

					if(utils.getSonsFromRecrod(pair.getLeft()).equals(sonsList)) {	
						logger.debug("Found ConsumerRecord for update : "+pair.getLeft());
						logger.debug("timestamp : "+pair.getRight());
						return pair.getRight();
					}
				}
			}
			else {
				ConsumerRecords<Object, Object> records = updateConsumer.poll(10000);

				for (ConsumerRecord<Object, Object> param : records) {

					GenericRecord family = (GenericRecord)param.value(); 				
					if(utils.getSonsFromRecrod(family).equals(sonsList)) {					
						logger.debug("Found ConsumerRecord for update : "+param);
						logger.debug("timestamp : "+param.timestamp());
						return param.timestamp();
					}
				}
			}
		}	
	} 
}
