package org.engine.process.performance.activity.create;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;  
import java.util.List;
import java.util.Map;  
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;  
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition; 
import org.apache.kafka.clients.producer.KafkaProducer; 
import org.apache.log4j.Logger; 
import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.Constant;
import org.engine.process.performance.utils.Pair;
import org.engine.process.performance.utils.Utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;

import joptsimple.internal.Strings;

/**
 * @author assafsh
 * Jul 2017
 * 
 * The class will check the performance of the engine by create messages to topics
 * sourceName-raw-data
 * update
 * get the message timestamp and compare
 *
 */

public class CreateActivityConsumer extends ActivityConsumer {

	private StringBuffer output = new StringBuffer();
	private String externalSystemID;
	private String sourceName;   
	private String endl = "\n"; 
	private List<Pair<String,Long>> rawDataRecordsList = new ArrayList<>(); 
	private List<Pair<GenericRecord,Long>> sourceRecordsList = new ArrayList<>(); 
	private List<Pair<GenericRecord,Long>> updateRecordsList = new ArrayList<>();
	private long lastOffsetForRawData;
	private long lastOffsetForUpdate;
	private long lastOffsetForSource;	
	private Pair<Long,Long> timeDifferences;
	private KafkaConsumer<Object, Object> consumer;
	private KafkaConsumer<Object, Object> consumer2;
	private KafkaConsumer<Object, Object> consumer3;
	private TopicPartition partitionRawData;
	private TopicPartition partitionSource;
	private TopicPartition partitionUpdate;
	private String identifierId;
	final static public Logger logger = Logger.getLogger(CreateActivityConsumer.class);

	static {
		Utils.setDebugLevel(logger);
	}
	
	public CreateActivityConsumer(String sourceName, String externalSystemID, String identifierId) {

		this.sourceName = sourceName; 
		this.externalSystemID = externalSystemID; 
		this.identifierId = identifierId;
		
		partitionRawData = new TopicPartition(sourceName+"-raw-data", 0);
		int partition = utils.getPartition(externalSystemID);
		partitionSource = new TopicPartition(sourceName, partition);
		partitionUpdate = new TopicPartition("update", 0);
	}
	
	public CreateActivityConsumer() {
		super();
	}

	public void handleMessage() throws IOException {
		
		rawDataRecordsList.clear();
		updateRecordsList.clear(); 
		sourceRecordsList.clear(); 
			
		Properties props = utils.getProperties(false); 
		Properties propsWithAvro = utils.getProperties(true); 
				
		consumer = new KafkaConsumer<Object, Object>(props);
		consumer.assign(Arrays.asList(partitionRawData));
		consumer.seekToEnd(Arrays.asList(partitionRawData));
		lastOffsetForRawData = consumer.position(partitionRawData); 

		consumer2 = new KafkaConsumer<Object, Object>(propsWithAvro);
		consumer2.assign(Arrays.asList(partitionUpdate));
		consumer2.seekToEnd(Arrays.asList(partitionUpdate));
		lastOffsetForUpdate = consumer2.position(partitionUpdate); 
		
		consumer3 = new KafkaConsumer<Object, Object>(propsWithAvro);
		consumer3.assign(Arrays.asList(partitionSource));
		consumer3.seekToEnd(Arrays.asList(partitionSource));
		lastOffsetForSource = consumer3.position(partitionSource); 

		output.append("The current offset before produce the message are ").append(endl);
		output.append(sourceName+"-raw-data : "+lastOffsetForRawData).append(endl);
		output.append(sourceName+" :"+lastOffsetForSource).append(endl);
		output.append("update :"+lastOffsetForUpdate).append(endl);
 
		try(KafkaProducer<Object, Object> producer = new KafkaProducer<>(props)) {
			ProducerRecord<Object, Object> record = new ProducerRecord<>(sourceName+"-raw-data",externalSystemID,getJsonGenericRecord(identifierId));
			producer.send(record); 

		}
	} 
	
	@Override
	public void callConsumer() throws Exception {

		consumer.seek(partitionRawData, lastOffsetForRawData);
		callConsumersWithKafkaConsuemr(consumer);

		consumer2.seek(partitionUpdate, lastOffsetForUpdate);
		callConsumersWithKafkaConsuemr(consumer2); 
		
		consumer3.seek(partitionSource, lastOffsetForSource);
		callConsumersWithKafkaConsuemr(consumer3);
		
		Pair<GenericRecord,Long> update = updateRecordsList.stream().collect(Collectors.toList()).get(0);
		logger.debug("====Consumer from topic update: "+update.toString());
		Pair<GenericRecord,Long> source = sourceRecordsList.stream().collect(Collectors.toList()).get(0);
		logger.debug("====Consumer from topic source: "+source.toString());
		Pair<String,Long> rowData = rawDataRecordsList.stream().collect(Collectors.toList()).get(0);
		logger.debug("====Consumer from topic "+sourceName+"-raw-data: "+rowData.toString());
		
		timeDifferences = new Pair<Long,Long>(update.getRight() - source.getRight(), source.getRight() - rowData.getRight());	
	} 
	
	
	public Pair<Long,Long> getTimeDifferences() throws Exception {
		
		return timeDifferences; 
	}	


	public long getLastOffsetForRawData() {
		return lastOffsetForRawData;
	}

	public void setLastOffsetForRawData(long lastOffsetForRawData) {
		this.lastOffsetForRawData = lastOffsetForRawData;
	}

	public long getLastOffsetForUpdate() {
		return lastOffsetForUpdate;
	}

	public void setLastOffsetForUpdate(long lastOffsetForUpdate) {
		this.lastOffsetForUpdate = lastOffsetForUpdate;
	}

	public long getLastOffsetForSource() {
		return lastOffsetForSource;
	}

	public void setLastOffsetForSource(long lastOffsetForSource) {
		this.lastOffsetForSource = lastOffsetForSource;
	}
	
	private String getJsonGenericRecord(String identifierId) {

		/*
		 * 
		 * {"basicAttributes": {"coordinate": {"lat": 4.5, "long": 3.4}, "isNotTracked": false, "entityOffset": 50, "sourceName": "source1"},
		 *  "speed": 4.7, "elevation": 7.8, 
		 * "course": 8.3, "nationality": "USA", "category": "boat", "pictureURL": "huh?", "height": 6.1, 
		 * "nickname": "rerere", "externalSystemID": "id1"}
		 * 
		 */

		String timestamp = Long.toString(System.currentTimeMillis());
		
		return   "{\"id\":\""+externalSystemID+"\"," 
		+"\"metadata\":\""+Constant.CREATE_IDENTIFIER_TYPE+"="+identifierId+"\"," 		
		+"\"lat\": 222," 
		+"\"xlong\": 333," 
		+"\"source_name\":\""+sourceName+"\"," 
		+"\"category\":\"boat\","
		+"\"speed\":\"444\", "
		+"\"course\":\"5.55\", "
		+"\"elevation\":\"7.8\"," 
		+"\"nationality\":\"USA\"," 
		+"\"picture_url\":\"URL\", "
		+"\"height\":\"44\","
		+"\"nickname\":\"mick\"," 
		+" \"timestamp\":\""+timestamp+"\"  }";
	} 
	
	private void callConsumersWithKafkaConsuemr(KafkaConsumer<Object, Object> consumer) throws JsonParseException, JsonMappingException, IOException {

		boolean isRunning = true;
		String metadata = null;
		while (isRunning) {
			ConsumerRecords<Object, Object> records = consumer.poll(10000);

			for (ConsumerRecord<Object, Object> param : records) {
				if( param.topic().equals("update") ||  param.topic().equals(sourceName)) {
					GenericRecord record = (GenericRecord)param.value();
					metadata =  (String )record.get("metadata");
				}
				else {
					Map<String,String> map = jsonToMap((String)param.value()); 
					metadata = map.get("metadata"); 
				}
				
				if(Strings.isNullOrEmpty(metadata))
					continue;
				
				if(metadata.contains(Constant.CREATE_IDENTIFIER_TYPE+"="+identifierId)) {

					if( param.topic().equals("update")) {
						updateRecordsList.add(new Pair<GenericRecord,Long>((GenericRecord)param.value(),param.timestamp()));
					}
					else if(param.topic().equals(sourceName)) {
						sourceRecordsList.add(new Pair<GenericRecord,Long>((GenericRecord)param.value(),param.timestamp()));
					}
					else {
						rawDataRecordsList.add(new Pair<String,Long>((String)param.value(),param.timestamp()));
					}
					isRunning = false;
					consumer.close();
					break;
				}
			}
		}
	}
	
	private Map<String,String> jsonToMap(String json) throws JsonParseException, JsonMappingException, IOException {
		
		ObjectMapper mapper = new ObjectMapper();
		Map<String, String> map = mapper.readValue(json, new TypeReference<Map<String,String>>() { });
		
		return map;
	} 
 
 }
