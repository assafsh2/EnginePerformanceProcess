package org.engine.process.performance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

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

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.function.Procedure;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
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
 * sourceName (source1..)
 * update
 * get the message timestamp and compare
 *
 */
public class EnginePerformance extends InnerService {

	private StringBuffer output = new StringBuffer();
	private String externalSystemID;	
	private String kafkaAddress;
	private String schemaRegustryUrl; 
	private String schemaRegustryIdentity;
	private String sourceName; 
	private String endl = "\n";
	private final ActorSystem system = ActorSystem.create();
	private final ActorMaterializer materializer = ActorMaterializer.create(system);
	private SchemaRegistryClient schemaRegistry = null;
	private List<Pair<GenericRecord,Long>> sourceRecordsList = new ArrayList<>(); 
	private List<Pair<GenericRecord,Long>> updateRecordsList = new ArrayList<>();
	boolean testing;
	
	public EnginePerformance(String kafkaAddress, String schemaRegustryUrl, String schemaRegustryIdentity,String sourceName) {

		this.kafkaAddress = kafkaAddress;
		this.schemaRegustryUrl = schemaRegustryUrl;
		this.schemaRegustryIdentity = schemaRegustryIdentity;
		this.sourceName = sourceName; 
	}

	//ONLY FOR TESTING
	public EnginePerformance(String kafkaAddress,SchemaRegistryClient schemaRegistry,String sourceName) {
		
		this.sourceName = sourceName;
		this.schemaRegistry = schemaRegistry;
		this.sourceName = sourceName; 
		testing = true;
		this.kafkaAddress = kafkaAddress;
	}
	
	@Override
	public String getOutput() {
		return output.toString();
	}

	@Override
	public void preExecute() throws IOException, RestClientException {

		if(testing)
			return;
		
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
		System.out.println("after random "+externalSystemID); 
		handleCreateMessage();
		handleUpdateMessage();		

		return ServiceStatus.SUCCESS;
	}

	private void handleCreateMessage() throws IOException, RestClientException {

		//Akka Actor
		if(testing) {
			System.out.println("Create message with Akka Actor");

			ProducerSettings<String, Object> producerSettings = ProducerSettings
					.create(system, new StringSerializer(), new KafkaAvroSerializer(schemaRegistry))
					.withBootstrapServers(kafkaAddress);

			creationTopicProducer(producerSettings); 
			String lat = "44.9";
			String longX = "95.8";
			sourceTopicProducer(producerSettings,lat,longX);

			sourceRecordsList.clear();
			updateRecordsList.clear(); 

			callConsumers();


			long diffTime = getTimeDifferences(lat, longX);
			output.append("The create took "+diffTime +" millisec").append(endl);
		}
		//KafkaConsumer
		else {

			System.out.println("Create message with KafkaConsumer");

			Properties props = getProperties();

			String lat = "44.9";
			String longX = "95.8";
			TopicPartition partitionSource = new TopicPartition(sourceName, 0);
			TopicPartition partitionUpdate = new TopicPartition("update", 0);

			sourceRecordsList.clear();
			updateRecordsList.clear(); 
			long lastOffsetForSource;
			long lastOffsetForUpdate;

			KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(props);
			consumer.assign(Arrays.asList(partitionSource));
			consumer.seekToEnd(Arrays.asList(partitionSource));
			lastOffsetForSource = consumer.position(partitionSource); 

			System.out.println("Create message with KafkaConsumer2 "+lastOffsetForSource);
			
			KafkaConsumer<Object, Object> consumer2 = new KafkaConsumer<Object, Object>(props);
			consumer2.assign(Arrays.asList(partitionUpdate));
			consumer2.seekToEnd(Arrays.asList(partitionUpdate));
			lastOffsetForUpdate = consumer2.position(partitionUpdate); 
			
			System.out.println("Create message with KafkaConsumer3 "+lastOffsetForUpdate);

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}			

			try(KafkaProducer<Object, Object> producer = new KafkaProducer<>(props)) {

				ProducerRecord<Object, Object> record = new ProducerRecord<>("creation",getCreationGenericRecord());
				producer.send(record);

				ProducerRecord<Object, Object> record2 = new ProducerRecord<>(sourceName,getSourceGenericRecord(lat, longX));
				producer.send(record2);


			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}	

			System.out.println("Create message with KafkaConsumer5 "+lastOffsetForUpdate);
			consumer.seek(partitionSource, lastOffsetForSource);
			callConsumersWithKafkaConsuemr(consumer,lat,longX);

			System.out.println("Create message with KafkaConsumer6 "+lastOffsetForUpdate);
			consumer2.seek(partitionUpdate, lastOffsetForUpdate);
			callConsumersWithKafkaConsuemr(consumer2,lat,longX);

			System.out.println("Create message with KafkaConsumer7");
			
			long diffTime = getTimeDifferences(lat, longX);
			output.append("The create action between topics  <"+sourceName+"> and <update> took "+diffTime +" millisec").append(endl);
		}

	}

	private Properties getProperties() {

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroDeserializer.class);		
		props.put("schema.registry.url", schemaRegustryUrl);
		props.put("group.id", "group1");

		return props;
	}

	private GenericRecord getCreationGenericRecord() throws IOException, RestClientException {
		
		Schema creationSchema = getSchema("detectionEvent"); 
		GenericRecord creationRecord = new GenericRecordBuilder(creationSchema)
		.set("sourceName", sourceName)
		.set("externalSystemID",externalSystemID)
		.build();
		
		return creationRecord;
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
		//Akka Actor
		if(testing) {

			System.out.println("Update message with Akka Actor");
			
			ProducerSettings<String, Object> producerSettings = ProducerSettings
					.create(system, new StringSerializer(), new KafkaAvroSerializer(schemaRegistry))
					.withBootstrapServers(kafkaAddress);

			sourceTopicProducer(producerSettings,lat,longX);

			sourceRecordsList.clear();
			updateRecordsList.clear(); 

			callConsumers(); 
		}

		//KafkaConsumer
		else {

			System.out.println("Update message with KafkaConsumer");

			Properties props = getProperties(); 

			TopicPartition partitionSource = new TopicPartition(sourceName, 0);
			TopicPartition partitionUpdate = new TopicPartition("update", 0);

			sourceRecordsList.clear();
			updateRecordsList.clear(); 
			long lastOffsetForSource;
			long lastOffsetForUpdate;

			KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(props);
			consumer.assign(Arrays.asList(partitionSource));
			consumer.seekToEnd(Arrays.asList(partitionSource));
			lastOffsetForSource = consumer.position(partitionSource); 
			
			

			KafkaConsumer<Object, Object> consumer2 = new KafkaConsumer<Object, Object>(props);
			consumer2.assign(Arrays.asList(partitionUpdate));
			consumer2.seekToEnd(Arrays.asList(partitionUpdate));
			lastOffsetForUpdate = consumer2.position(partitionUpdate); 

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}			

			try(KafkaProducer<Object, Object> producer = new KafkaProducer<>(props)) {

				ProducerRecord<Object, Object> record2 = new ProducerRecord<>(sourceName,getSourceGenericRecord(lat, longX));
				producer.send(record2);
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}	

			consumer.seek(partitionSource, lastOffsetForSource);
			callConsumersWithKafkaConsuemr(consumer,lat,longX);

			consumer2.seek(partitionUpdate, lastOffsetForUpdate);
			callConsumersWithKafkaConsuemr(consumer2,lat,longX);

		}
		
		long diffTime = getTimeDifferences(lat, longX);
		output.append("The update action between topics  <"+sourceName+"> and <update> took "+diffTime +" millisec").append(endl);
	}
	
	private void callConsumers() {		 
		
		KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer(schemaRegistry);
		keyDeserializer.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), true);

		final ConsumerSettings<String, Object> consumerSettings =
				ConsumerSettings.create(system, new StringDeserializer(), keyDeserializer)
				.withBootstrapServers(kafkaAddress)
				.withGroupId("group1")
				.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		Procedure<ConsumerRecord<String, Object>> f = new Procedure<ConsumerRecord<String, Object>>() {

			private static final long serialVersionUID = 1L;

			public void apply(ConsumerRecord<String, Object> param)
					throws Exception {

				//System.out.println("Topic is: "+param.topic()+" timestamp is: "+param.timestamp() + 
				//		" value is: "+ param.value());
				if( param.topic().equals("update")) {
					updateRecordsList.add(new Pair<GenericRecord,Long>((GenericRecord)param.value(),param.timestamp()));
				}
				else {
					sourceRecordsList.add(new Pair<GenericRecord,Long>((GenericRecord)param.value(),param.timestamp()));
				}
			}
		};

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("\n\n\n====="+sourceName);		

		Consumer.committableSource(consumerSettings, Subscriptions.topics(sourceName))
		.map(result -> result.record()).runForeach(f, materializer);

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("=====update");
		Consumer.committableSource(consumerSettings, Subscriptions.topics("update"))
		.map(result -> result.record()).runForeach(f, materializer);
	}
	
	
	private void callConsumersWithKafkaConsuemr(KafkaConsumer<Object, Object> consumer,String lat,String longX) {
		
		boolean isRunning = true;
		while (isRunning) {
			ConsumerRecords<Object, Object> records = consumer.poll(10000);

			for (ConsumerRecord<Object, Object> param : records) {

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
						sourceRecordsList.add(new Pair<GenericRecord,Long>((GenericRecord)param.value(),param.timestamp()));
					}
					isRunning = false;
					consumer.close();
					break;
				}

			}
		}
	}
	
	private void sourceTopicProducer(ProducerSettings<String, Object> producerSettings, String lat, String longX) throws IOException, RestClientException {
		
		Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer.plainSink(producerSettings);

		ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>(sourceName, getSourceGenericRecord(lat, longX));

		Source.from(Arrays.asList(producerRecord))
		.to(sink)
		.run(materializer); 
		
	}
	
	private void creationTopicProducer(ProducerSettings<String, Object> producerSettings) throws IOException, RestClientException {
		Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer.plainSink(producerSettings);
		
		ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>("creation", getCreationGenericRecord());

		Source.from(Arrays.asList(producerRecord))
		.to(sink)
		.run(materializer);
	}


	private long getTimeDifferences(String inputLat,String inputLongX) {

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Predicate<Pair<GenericRecord,Long>> predicate = record -> {  

			GenericRecord entityAttributes =  ((GenericRecord) record.first().get("entityAttributes"));	
			GenericRecord basicAttributes = (entityAttributes != null) ? ((GenericRecord) entityAttributes.get("basicAttributes")) : ((GenericRecord) record.first().get("basicAttributes"));
			String externalSystemIDTmp = (entityAttributes != null) ? entityAttributes.get("externalSystemID").toString() : record.first().get("externalSystemID").toString();
			GenericRecord coordinate = (GenericRecord)basicAttributes.get("coordinate");			
			String lat = coordinate.get("lat").toString(); 
			String longX = coordinate.get("long").toString();

			return externalSystemIDTmp.equals(externalSystemID) && lat.equals(inputLat) &&  longX.equals(inputLongX);		 
		};

		Pair<GenericRecord,Long> update = updateRecordsList.stream().filter(predicate).collect(Collectors.toList()).get(0);
		System.out.println("Consumer from topic update: "+update.toString());
		Pair<GenericRecord,Long> source = sourceRecordsList.stream().filter(predicate).collect(Collectors.toList()).get(0);
		System.out.println("Consumer from topic "+sourceName+": "+source.toString());
		
		return update.second() - source.second();	
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
		if(!testing) {
			name = "org.sourcestream.entities."+name;
		}
		int id = schemaRegistry.getLatestSchemaMetadata(name).getId();
		return schemaRegistry.getByID(id);
	}
}
 
