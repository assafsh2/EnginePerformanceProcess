package org.engine.process.performance.multi;

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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import akka.japi.Pair;  
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException; 

import org.apache.kafka.clients.producer.ProducerConfig; 
import org.apache.kafka.clients.producer.KafkaProducer; 

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
 * sourceName-row-data
 * update
 * get the message timestamp and compare
 *
 */

public class HandlePerformanceMessages {

	private StringBuffer output = new StringBuffer();
	private String externalSystemID;	
	private String kafkaAddress;
	private String schemaRegustryUrl;  
	private String sourceName;   
	private String endl = "\n"; 
	private List<Pair<String,Long>> rawDataRecordsList = new ArrayList<>(); 
	private List<Pair<GenericRecord,Long>> sourceRecordsList = new ArrayList<>(); 
	private List<Pair<GenericRecord,Long>> updateRecordsList = new ArrayList<>();
	private long lastOffsetForRawData;
	private long lastOffsetForUpdate;
	private long lastOffsetForSource;
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

	private KafkaConsumer<Object, Object> consumer;
	private KafkaConsumer<Object, Object> consumer2;
	private KafkaConsumer<Object, Object> consumer3;
	private TopicPartition partitionRawData;
	private TopicPartition partitionSource;
	private TopicPartition partitionUpdate;
	private String lat;
	private String longX;
	

	public HandlePerformanceMessages(String kafkaAddress, String schemaRegustryUrl, String sourceName, String externalSystemID, String lat , String longX) {

		this.kafkaAddress = kafkaAddress;
		this.schemaRegustryUrl = schemaRegustryUrl; 
		this.sourceName = sourceName; 
		this.externalSystemID = externalSystemID; 
		this.lat = lat;
		this.longX = longX; 
		partitionRawData = new TopicPartition(sourceName+"-raw-data", 0);
		partitionSource = new TopicPartition(sourceName, 0);
		partitionUpdate = new TopicPartition("update", 0);
	}

	public void handleMessage() throws IOException, RestClientException {
		
		rawDataRecordsList.clear();
		updateRecordsList.clear(); 
		sourceRecordsList.clear(); 
			
		Properties props = getProperties(false); 
		Properties propsWithAvro = getProperties(true); 
				
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

	private Properties getProperties(boolean isAvro) {

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		if(isAvro) {
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					io.confluent.kafka.serializers.KafkaAvroSerializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
		}
		else {
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class);	
		}
 	
		props.put("schema.registry.url", schemaRegustryUrl);
		props.put("group.id", "group1");

		return props;
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

	public Pair<Long,Long> getTimeDifferences() throws Exception {

		consumer.seek(partitionRawData, lastOffsetForRawData);
		callConsumersWithKafkaConsuemr(consumer);

		consumer2.seek(partitionUpdate, lastOffsetForUpdate);
		callConsumersWithKafkaConsuemr(consumer2); 
		
		consumer3.seek(partitionSource, lastOffsetForSource);
		callConsumersWithKafkaConsuemr(consumer3);
		
		Pair<GenericRecord,Long> update = updateRecordsList.stream().collect(Collectors.toList()).get(0);
		System.out.println("====Consumer from topic update: "+update.toString());
		Pair<GenericRecord,Long> source = sourceRecordsList.stream().collect(Collectors.toList()).get(0);
		System.out.println("====Consumer from topic source: "+source.toString());
		Pair<String,Long> rowData = rawDataRecordsList.stream().collect(Collectors.toList()).get(0);
		System.out.println("====Consumer from topic "+sourceName+"-row-data: "+rowData.toString());

		return new Pair<Long,Long>(update.second() - source.second(), source.second() - rowData.second());	
	} 
	
	private Map<String,String> jsonToMap(String json) throws JsonParseException, JsonMappingException, IOException {
		
		ObjectMapper mapper = new ObjectMapper();
		Map<String, String> map = mapper.readValue(json, new TypeReference<Map<String,String>>() { });
		
		return map;
	} 
 
 }
