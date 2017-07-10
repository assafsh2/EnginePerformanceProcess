package org.engine.process.performance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays; 
import java.util.List;
import java.util.Random; 
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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

public class EnginePerformanceFromBegining extends InnerService {

	private String[] output = new String[2];
	private String externalSystemID;	
	private String kafkaAddress;
	private String schemaRegustryUrl; 
	private String schemaRegustryIdentity;
	private String sourceName;   
	private SchemaRegistryClient schemaRegistry = null;
	private List<Pair<GenericRecord,Long>> rawDataRecordsList = new ArrayList<>(); 
	private List<Pair<GenericRecord,Long>> updateRecordsList = new ArrayList<>();

	public EnginePerformanceFromBegining(String kafkaAddress, String schemaRegustryUrl, String schemaRegustryIdentity,String sourceName) {

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
		handleCreateMessage();
		//handleUpdateMessage();		

		return ServiceStatus.SUCCESS;
	}

	private void handleCreateMessage() throws IOException, RestClientException {

		Properties props = getProperties();

		String lat = "44.9";
		String longX = "95.8";
		TopicPartition partitionRawData = new TopicPartition(sourceName+"-raw-data", 0);
		TopicPartition partitionUpdate = new TopicPartition("update", 0);

		rawDataRecordsList.clear();
		updateRecordsList.clear(); 
		long lastOffsetForRawData;
		long lastOffsetForUpdate;
		
		
		System.out.println("AAAA");
		System.out.println(props);
		
		
		KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(props);
		System.out.println("AAAA1");
		consumer.assign(Arrays.asList(partitionRawData));
		System.out.println("AAAA2");
		consumer.seekToEnd(Arrays.asList(partitionRawData));
		System.out.println("AAAA2");
		lastOffsetForRawData = consumer.position(partitionRawData); 
		
		System.out.println("BBBB");

		KafkaConsumer<Object, Object> consumer2 = new KafkaConsumer<Object, Object>(props);
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

		long diffTime = getTimeDifferences(lat, longX);
		output[0] = "The create action between topics  <"+sourceName+"-row-data> and <update> took "+diffTime +" millisec";
		System.out.println(output[0]); 
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

		return   "{\"id\":\""+externalSystemID+"\"," 
		+"\"lat\":\""+lat+"\"," 
		+"\"xlong\":\""+longX+"\"," 
		+"\"source_name\":\""+sourceName+"\"," 
		+"\"category\":\"boat\","
		+"\"speed\":\"444\", "
		+"\"course\":\"5.55\", "
		+"\"elevation\":\"dd\"," 
		+"\"nationality\":\"USA\"," 
		+"\"picture_url\":\"URL\", "
		+"\"height\":\"44\","
		+"\"nickname\":\"mick\"," 
		+" \"timestamp\":\"dd\"  }";
	}

	private Properties getProperties() {

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);		
		props.put("schema.registry.url", schemaRegustryUrl);
		props.put("group.id", "group1");

		return props;
	}


	private GenericRecord getSourceGenericRecord(String lat, String longX) throws IOException, RestClientException {

		Schema basicAttributesSchema = getSchema("basicEntityAttributes");
		Schema coordinateSchema = basicAttributesSchema.getField("coordinate").schema();

		GenericRecord coordinate = new GenericRecordBuilder(coordinateSchema)
		.set("lat", Double.parseDouble(lat))
		.set("long",Double.parseDouble(longX))
		.build();

		GenericRecord basicAttributes = new GenericRecordBuilder(basicAttributesSchema)
		.set("coordinate", coordinate)
		.set("isNotTracked", false)
		.set("entityOffset", 50l)
		.set("sourceName",sourceName)
		.build();		 

		Schema dataSchema = getSchema("generalEntityAttributes");
		Schema nationalitySchema = dataSchema.getField("nationality").schema();
		Schema categorySchema = dataSchema.getField("category").schema();
		GenericRecord dataRecord = new GenericRecordBuilder(dataSchema)
		.set("basicAttributes", basicAttributes)
		.set("speed", 4.7)
		.set("elevation", 7.8)
		.set("course", 8.3)
		.set("nationality", new GenericData.EnumSymbol(nationalitySchema, "USA"))
		.set("category", new GenericData.EnumSymbol(categorySchema, "boat"))
		.set("pictureURL", "huh?")
		.set("height", 6.1)
		.set("nickname", "rerere")
		.set("externalSystemID", externalSystemID)
		.build();


		return dataRecord;
	}

	private void handleUpdateMessage() throws IOException, RestClientException {

		String lat = "34.66";
		String longX = "48.66";


		System.out.println("Update message with KafkaConsumer");

		Properties props = getProperties(); 

		TopicPartition partitionRawData = new TopicPartition(sourceName+"-raw-data", 0);
		TopicPartition partitionUpdate = new TopicPartition("update", 0);

		rawDataRecordsList.clear();
		updateRecordsList.clear(); 
		long lastOffsetForRawData;
		long lastOffsetForUpdate;

		KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(props);
		consumer.assign(Arrays.asList(partitionRawData));
		consumer.seekToEnd(Arrays.asList(partitionRawData));
		lastOffsetForRawData = consumer.position(partitionRawData); 

		KafkaConsumer<Object, Object> consumer2 = new KafkaConsumer<Object, Object>(props);
		consumer2.assign(Arrays.asList(partitionUpdate));
		consumer2.seekToEnd(Arrays.asList(partitionUpdate));
		lastOffsetForUpdate = consumer2.position(partitionUpdate); 

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}			

		try(KafkaProducer<Object, Object> producer = new KafkaProducer<>(props)) {

			ProducerRecord<Object, Object> record2 = new ProducerRecord<>(sourceName+"-raw-data",getSourceGenericRecord(lat, longX));
			producer.send(record2);
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}	

		consumer.seek(partitionRawData, lastOffsetForRawData);
		callConsumersWithKafkaConsuemr(consumer,lat,longX);

		consumer2.seek(partitionUpdate, lastOffsetForUpdate);
		callConsumersWithKafkaConsuemr(consumer2,lat,longX);


		long diffTime = getTimeDifferences(lat, longX);
		output[1] = "The update action between topics  <"+sourceName+"-raw-data> and <update> took "+diffTime +" millisec";
		System.out.println(output[1]); 
	}

 

	private void callConsumersWithKafkaConsuemr(KafkaConsumer<Object, Object> consumer,String lat,String longX) {

		boolean isRunning = true;
		while (isRunning) {
			ConsumerRecords<Object, Object> records = consumer.poll(10000);

			for (ConsumerRecord<Object, Object> param : records) {
				
				System.out.println("*******"+ param);

				GenericRecord record = (GenericRecord)param.value();

				GenericRecord entityAttributes =  ((GenericRecord) record.get("entityAttributes"));	
				GenericRecord basicAttributes = (entityAttributes != null) ? ((GenericRecord) entityAttributes.get("basicAttributes")) : ((GenericRecord) record.get("basicAttributes"));
				String externalSystemIDTmp = (entityAttributes != null) ? entityAttributes.get("externalSystemID").toString() : record.get("externalSystemID").toString();
				GenericRecord coordinate = (GenericRecord)basicAttributes.get("coordinate");			
				String latTmp = coordinate.get("lat").toString(); 
				String longXTmp = coordinate.get("long").toString();

				if( externalSystemIDTmp.equals(externalSystemID) && lat.equals(latTmp) &&  longX.equals(longXTmp)) {

					if( param.topic().equals("update")) {
						updateRecordsList.add(new Pair<GenericRecord,Long>((GenericRecord)param.value(),param.timestamp()));
					}
					else {
						rawDataRecordsList.add(new Pair<GenericRecord,Long>((GenericRecord)param.value(),param.timestamp()));
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

		Predicate<Pair<GenericRecord,Long>> predicateUpdate = record -> {  
	
			GenericRecord entityAttributes =  ((GenericRecord) record.first().get("entityAttributes"));	
			GenericRecord basicAttributes = (entityAttributes != null) ? ((GenericRecord) entityAttributes.get("basicAttributes")) : ((GenericRecord) record.first().get("basicAttributes"));
			String externalSystemIDTmp = (entityAttributes != null) ? entityAttributes.get("externalSystemID").toString() : record.first().get("externalSystemID").toString();
			GenericRecord coordinate = (GenericRecord)basicAttributes.get("coordinate");			
			String lat = coordinate.get("lat").toString(); 
			String longX = coordinate.get("long").toString();

			return externalSystemIDTmp.equals(externalSystemID) && lat.equals(inputLat) &&  longX.equals(inputLongX);		 
		};

		Predicate<Pair<GenericRecord,Long>> predicateRowData = record -> {  
			
			System.out.println(record);
			/*
			GenericRecord entityAttributes =  ((GenericRecord) record.first().get("entityAttributes"));	
			GenericRecord basicAttributes = (entityAttributes != null) ? ((GenericRecord) entityAttributes.get("basicAttributes")) : ((GenericRecord) record.first().get("basicAttributes"));
			String externalSystemIDTmp = (entityAttributes != null) ? entityAttributes.get("externalSystemID").toString() : record.first().get("externalSystemID").toString();
			GenericRecord coordinate = (GenericRecord)basicAttributes.get("coordinate");			
			String lat = coordinate.get("lat").toString(); 
			String longX = coordinate.get("long").toString();
*/
			//return externalSystemIDTmp.equals(externalSystemID) && lat.equals(inputLat) &&  longX.equals(inputLongX);
			return true;
		};	
		
		Pair<GenericRecord,Long> update = updateRecordsList.stream().filter(predicateUpdate).collect(Collectors.toList()).get(0);
		System.out.println("Consumer from topic update: "+update.toString());
		Pair<GenericRecord,Long> rowData = rawDataRecordsList.stream().filter(predicateRowData).collect(Collectors.toList()).get(0);
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

	private Schema getSchema(String name) throws IOException, RestClientException {

		name = "org.sourcestream.entities."+name;

		int id = schemaRegistry.getLatestSchemaMetadata(name).getId();
		return schemaRegistry.getByID(id);
	}
}
