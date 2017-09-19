package org.engine.process.performance.activity.create;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;  
import java.util.List;
import java.util.Map;  
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord; 
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition; 
import org.apache.kafka.clients.producer.KafkaProducer; 
import org.apache.log4j.Logger;
import org.engine.process.performance.Main;
import org.engine.process.performance.activity.merge.MergeActivityConsumer;
import org.engine.process.performance.utils.ActivityConsumer;
import org.engine.process.performance.utils.Pair;
import org.engine.process.performance.utils.Utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;

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
	private String lat;
	private String longX;	

	final static public Logger logger = Logger.getLogger(CreateActivityConsumer.class);

	static {
		Utils.setDebugLevel(System.getenv("DEBUG_LEVEL"),logger);
	}
	
	public CreateActivityConsumer(String sourceName, String externalSystemID, String lat , String longX) {

		this.sourceName = sourceName; 
		this.externalSystemID = externalSystemID; 
		this.lat = lat;
		this.longX = longX; 
		partitionRawData = new TopicPartition(sourceName+"-raw-data", 0);
		partitionSource = new TopicPartition(sourceName, 0);
		partitionUpdate = new TopicPartition("update", 0);
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
			ProducerRecord<Object, Object> record = new ProducerRecord<>(sourceName+"-raw-data",getJsonGenericRecord(lat,longX));
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
	
	private String getJsonGenericRecord(String lat,String longX) {

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
		+"\"lat\":\""+lat+"\"," 
		+"\"xlong\":\""+longX+"\"," 
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
		while (isRunning) {
			ConsumerRecords<Object, Object> records = consumer.poll(10000);

			for (ConsumerRecord<Object, Object> param : records) {
				
				String latTmp = null;
				String longXTmp = null;
				String externalSystemIDTmp = null;
				
				if( param.topic().equals("update") ||  param.topic().equals(sourceName)) {
					
					GenericRecord record = (GenericRecord)param.value();

					GenericRecord entityAttributes =  ((GenericRecord) record.get("entityAttributes"));	
					GenericRecord basicAttributes = (entityAttributes != null) ? ((GenericRecord) entityAttributes.get("basicAttributes")) : ((GenericRecord) record.get("basicAttributes"));
					externalSystemIDTmp = (entityAttributes != null) ? entityAttributes.get("externalSystemID").toString() : record.get("externalSystemID").toString();
					GenericRecord coordinate = (GenericRecord)basicAttributes.get("coordinate");			
					latTmp = coordinate.get("lat").toString(); 
					longXTmp = coordinate.get("long").toString();				
					
				}
				else {
					Map<String,String> map = jsonToMap((String)param.value()); 
					latTmp = map.get("lat"); 
					longXTmp =  map.get("xlong");
					externalSystemIDTmp = map.get("id"); 
				}
				
				if( externalSystemIDTmp.equals(externalSystemID) && lat.equals(latTmp) &&  longX.equals(longXTmp)) {

					if( param.topic().equals("update")) {
						updateRecordsList.add(new Pair<GenericRecord,Long>((GenericRecord)param.value(),param.timestamp()));
					}
					else if( param.topic().equals(sourceName)) {
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
