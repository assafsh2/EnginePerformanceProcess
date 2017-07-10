package org.engine.process.performance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;  
import java.util.List;
import java.util.Map;
import java.util.Random;  
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import org.apache.avro.Schema; 
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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
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

public class EnginePerformanceFromBeginning extends InnerService {

	private String[] output = new String[2];
	private String externalSystemID;	
	private String kafkaAddress;
	private String schemaRegustryUrl; 
	private String schemaRegustryIdentity;
	private String sourceName;   
	private SchemaRegistryClient schemaRegistry = null;
	private List<Pair<String,Long>> rawDataRecordsList = new ArrayList<>(); 
	private List<Pair<GenericRecord,Long>> updateRecordsList = new ArrayList<>();

	public EnginePerformanceFromBeginning(String kafkaAddress, String schemaRegustryUrl, String schemaRegustryIdentity,String sourceName) {

		this.kafkaAddress = kafkaAddress;
		this.schemaRegustryUrl = schemaRegustryUrl;
		this.schemaRegustryIdentity = schemaRegustryIdentity;
		this.sourceName = sourceName; 
	}

	public String[] getOutput() {
		return output;
	}

	@Override
	public void preExecute() throws IOException, RestClientException {

		if(kafkaAddress == null) {

			this.kafkaAddress = "localhost:9092";
		}

		if(schemaRegustryUrl != null) {

			schemaRegistry = new CachedSchemaRegistryClient(schemaRegustryUrl, Integer.parseInt(schemaRegustryIdentity));
		}
		else {
			schemaRegistry = new MockSchemaRegistryClient();
			registerSchemas(schemaRegistry);
		}
	}

	@Override
	public void postExecute() {

	}

	@Override
	public ServiceStatus execute() throws IOException, RestClientException {
		randomExternalSystemID();
		System.out.println("After random "+externalSystemID); 
		
		System.out.println("Create message with KafkaConsumer");
		handleMessage("44.9","95.8");
		
		long diffTime = getTimeDifferences("44.9", "95.8");
		output[0] = "The create action between topics  <"+sourceName+"-row-data> and <update> took "+diffTime +" millisec";
		System.out.println(output[0]);
		
		
		System.out.println("Update message with KafkaConsumer");
		handleMessage("34.66","48.66");
		
		diffTime = getTimeDifferences("34.66","48.66");
		output[1] = "The update action between topics  <"+sourceName+"-row-data> and <update> took "+diffTime +" millisec";
		System.out.println(output[1]); 
		
		return ServiceStatus.SUCCESS;
	}

	private void handleMessage(String lat,String longX) throws IOException, RestClientException {
		
		TopicPartition partitionRawData = new TopicPartition(sourceName+"-raw-data", 0);
		TopicPartition partitionUpdate = new TopicPartition("update", 0);

		rawDataRecordsList.clear();
		updateRecordsList.clear(); 
		long lastOffsetForRawData;
		long lastOffsetForUpdate;
		
		Properties props = getProperties(false); 
		Properties propsWithAvro = getProperties(true); 
		
		System.out.println("AAAA"); 		
		
		KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(props);
		System.out.println("AAAA1");
		consumer.assign(Arrays.asList(partitionRawData));
		System.out.println("AAAA2");
		consumer.seekToEnd(Arrays.asList(partitionRawData));
		System.out.println("AAAA2");
		lastOffsetForRawData = consumer.position(partitionRawData); 
		
		System.out.println("BBBB");

		KafkaConsumer<Object, Object> consumer2 = new KafkaConsumer<Object, Object>(propsWithAvro);
		System.out.println("BBB1");
		consumer2.assign(Arrays.asList(partitionUpdate));
		System.out.println("BBB2");
		consumer2.seekToEnd(Arrays.asList(partitionUpdate));
		System.out.println("BBB3");
		lastOffsetForUpdate = consumer2.position(partitionUpdate); 
		System.out.println("BBB4");

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}			

		try(KafkaProducer<Object, Object> producer = new KafkaProducer<>(props)) {

			ProducerRecord<Object, Object> record = new ProducerRecord<>(sourceName+"-raw-data",getJsonGenericRecord(lat,longX));
			producer.send(record); 

		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}	

		consumer.seek(partitionRawData, lastOffsetForRawData);
		callConsumersWithKafkaConsuemr(consumer,lat,longX);

		consumer2.seek(partitionUpdate, lastOffsetForUpdate);
		callConsumersWithKafkaConsuemr(consumer2,lat,longX); 
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

	private void callConsumersWithKafkaConsuemr(KafkaConsumer<Object, Object> consumer,String lat,String longX) throws JsonParseException, JsonMappingException, IOException {

		boolean isRunning = true;
		while (isRunning) {
			ConsumerRecords<Object, Object> records = consumer.poll(10000);

			for (ConsumerRecord<Object, Object> param : records) {
				
				String latTmp = null;
				String longXTmp = null;
				String externalSystemIDTmp = null;
				
				if( param.topic().equals("update")) {
					
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

	private long getTimeDifferences(String inputLat,String inputLongX) {

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	
		Pair<GenericRecord,Long> update = updateRecordsList.stream().collect(Collectors.toList()).get(0);
		System.out.println("Consumer from topic update: "+update.toString());
		Pair<String,Long> rowData = rawDataRecordsList.stream().collect(Collectors.toList()).get(0);
		System.out.println("Consumer from topic "+sourceName+"-row-data: "+rowData.toString());

		return update.second() - rowData.second();	
	}

	private void randomExternalSystemID() {

		Random r = new Random(); 
		externalSystemID = "performanceProcess-"+r.nextInt(10000);		
	} 


	private void registerSchemas(SchemaRegistryClient schemaRegistry) throws IOException, RestClientException {
		Schema.Parser parser = new Schema.Parser();
		schemaRegistry.register("detectionEvent",
				parser.parse("{\"type\": \"record\", "
						+ "\"name\": \"detectionEvent\", "
						+ "\"doc\": \"This is a schema for entity detection report event\", "
						+ "\"fields\": ["
						+ "{ \"name\": \"sourceName\", \"type\": \"string\", \"doc\" : \"interface name\" }, "
						+ "{ \"name\": \"externalSystemID\", \"type\": \"string\", \"doc\":\"external system ID\"}"
						+ "]}"));
		schemaRegistry.register("basicEntityAttributes",
				parser.parse("{\"type\": \"record\","
						+ "\"name\": \"basicEntityAttributes\","
						+ "\"doc\": \"This is a schema for basic entity attributes, this will represent basic entity in all life cycle\","
						+ "\"fields\": ["
						+ "{\"name\": \"coordinate\", \"type\":"
						+ "{\"type\": \"record\","
						+ "\"name\": \"coordinate\","
						+ "\"doc\": \"Location attribute in grid format\","
						+ "\"fields\": ["
						+ "{\"name\": \"lat\",\"type\": \"double\"},"
						+ "{\"name\": \"long\",\"type\": \"double\"}"
						+ "]}},"
						+ "{\"name\": \"isNotTracked\",\"type\": \"boolean\"},"
						+ "{\"name\": \"entityOffset\",\"type\": \"long\"},"
						+ "{\"name\": \"sourceName\", \"type\": \"string\"}"	
						+ "]}"));
		schemaRegistry.register("generalEntityAttributes",
				parser.parse("{\"type\": \"record\", "
						+ "\"name\": \"generalEntityAttributes\","
						+ "\"doc\": \"This is a schema for general entity before acquiring by the system\","
						+ "\"fields\": ["
						+ "{\"name\": \"basicAttributes\",\"type\": \"basicEntityAttributes\"},"
						+ "{\"name\": \"speed\",\"type\": \"double\",\"doc\" : \"This is the magnitude of the entity's velcity vector.\"},"
						+ "{\"name\": \"elevation\",\"type\": \"double\"},"
						+ "{\"name\": \"course\",\"type\": \"double\"},"
						+ "{\"name\": \"nationality\",\"type\": {\"name\": \"nationality\", \"type\": \"enum\",\"symbols\" : [\"ISRAEL\", \"USA\", \"SPAIN\"]}},"
						+ "{\"name\": \"category\",\"type\": {\"name\": \"category\", \"type\": \"enum\",\"symbols\" : [\"airplane\", \"boat\"]}},"
						+ "{\"name\": \"pictureURL\",\"type\": \"string\"},"
						+ "{\"name\": \"height\",\"type\": \"double\"},"
						+ "{\"name\": \"nickname\",\"type\": \"string\"},"
						+ "{\"name\": \"externalSystemID\",\"type\": \"string\",\"doc\" : \"This is ID given be external system.\"}"
						+ "]}"));


	}

	private Map<String,String> jsonToMap(String json) throws JsonParseException, JsonMappingException, IOException {
		
		ObjectMapper mapper = new ObjectMapper();
		Map<String, String> map = mapper.readValue(json, new TypeReference<Map<String,String>>() { });
		System.out.println(map);
		
		return map;
	}
 }
