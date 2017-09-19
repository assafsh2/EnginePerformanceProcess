package org.engine.process.performance.activity.merge;

import java.util.Arrays; 
import java.util.Set; 
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger; 
import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.Pair;
import org.engine.process.performance.utils.Utils;
import org.z.entities.schema.EntityFamily;
import org.z.entities.schema.MergeEvent;

public class MergeActivityConsumer extends ActivityConsumer{

	private long lastOffsetForMerge;
	private long lastOffsetForUpdate;
	private Set<Pair<String,String>> sonsList;
	private KafkaConsumer<Object, Object> mergeConsumer;
	private KafkaConsumer<Object, Object> updateConsumer;
	private TopicPartition partitionMerge; 
	private TopicPartition partitionUpdate;
	private long mergeToUpdateTimeDiff;
	private String metadata; 
	
	final static public Logger logger = Logger.getLogger(MergeActivityConsumer.class);

	static {
		Utils.setDebugLevel(System.getenv("DEBUG_LEVEL"),logger);
	}
 
	@Override
	public void callConsumer() {

		mergeConsumer = new KafkaConsumer<Object, Object>(utils.getProperties(true));
		mergeConsumer.assign(Arrays.asList(partitionMerge));
		mergeConsumer.seek(partitionMerge, lastOffsetForMerge);
		long mergeTimestamp = callMergeTopic();

		updateConsumer = new KafkaConsumer<Object, Object>(utils.getProperties(true));
		updateConsumer.assign(Arrays.asList(partitionUpdate));
		updateConsumer.seek(partitionUpdate, lastOffsetForUpdate);
		long updateTimestamp = callUpdateTopic();

		mergeToUpdateTimeDiff = updateTimestamp - mergeTimestamp;
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
	
	private long callMergeTopic() {

		while (true) {
			ConsumerRecords<Object, Object> records = mergeConsumer.poll(10000);

			for (ConsumerRecord<Object, Object> param : records) {

				MergeEvent mergeEvent = (MergeEvent)param.value(); 
				if(metadata.equals((String) mergeEvent.getMetadata())) {

					logger.debug("Found ConsumerRecord for merge : "+param);
					return param.timestamp();
				} 
			}
		}		 
	}

	private long callUpdateTopic() {

		while (true) {
			ConsumerRecords<Object, Object> records = updateConsumer.poll(10000);

			for (ConsumerRecord<Object, Object> param : records) {

				EntityFamily entityFamily = (EntityFamily)param.value();
				
				if(utils.getSonsFromRecrod(entityFamily).equals(sonsList)) {
					
					logger.debug("Found ConsumerRecord for merge : "+param);
					return param.timestamp();
				}
			}
		}	
	} 
}
