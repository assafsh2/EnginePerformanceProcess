package org.engine.process.performance.utils;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.util.ArrayList; 
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import org.apache.avro.Schema; 
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.engine.process.performance.Main; 

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.function.Procedure;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;


public abstract class InnerService {

	protected int interval;
	protected int durationInMin; 
	protected int numOfInteraces;
	protected int numOfUpdatesPerSec;
	protected int numOfCycles;
	protected int numOfUpdatesPerCycle;
	protected String kafkaAddress;
	protected String schemaRegustryUrl; 
	protected String schemaRegistryIdentity;
	protected String sourceName; 
	protected String endl = "\n";  
	protected List<SingleCycle> cyclesList; 
	protected String seperator = ",";
	protected String emptyString = "";

	abstract protected void preExecute() throws Exception;
	abstract protected void postExecute() throws Exception;
	abstract protected ServiceStatus execute() throws Exception;
	abstract public String getOutput();
	abstract public void printOutputToFile(String fileLocation);
	protected Utils utils;
	final static public Logger logger = Logger.getLogger(InnerService.class);
	static {
		Utils.setDebugLevel(logger);
	}

	public InnerService() {

		utils = new Utils(); 
		cyclesList = new ArrayList<>();

		if(Main.testing) {
			kafkaAddress = "192.168.0.51:9092 ";
			schemaRegustryUrl = "http://schema-registry.kafka:8081";
			schemaRegistryIdentity = "2021";
			sourceName = "source0"; 		
			numOfCycles = 1;
			numOfUpdatesPerCycle = 2; 
			interval = 500;
			durationInMin = 0;	
			numOfInteraces = 444;
			numOfUpdatesPerSec = 555; 
		}
		else {
			kafkaAddress = System.getenv("KAFKA_ADDRESS");
			schemaRegustryUrl = System.getenv("SCHEMA_REGISTRY_ADDRESS");
			schemaRegistryIdentity = System.getenv("SCHEMA_REGISTRY_IDENTITY");
			sourceName = System.getenv("SOURCE_NAME"); 		
			numOfCycles = Integer.parseInt(System.getenv("NUM_OF_CYCLES"));
			numOfUpdatesPerCycle = Integer.parseInt(System.getenv("NUM_OF_UPDATES_PER_CYCLE")); 
			interval = Integer.parseInt(System.getenv("INTERVAL"));
			durationInMin = Integer.parseInt(System.getenv("DURATION"));	
			numOfInteraces = Integer.parseInt(System.getenv("NUM_OF_INTERFACES"));
			numOfUpdatesPerSec = Integer.parseInt(System.getenv("NUM_OF_UPDATES")); 
		}
		logger.debug("KAFKA_ADDRESS::::::::" + kafkaAddress);
		logger.debug("SCHEMA_REGISTRY_ADDRESS::::::::" + schemaRegustryUrl);
		logger.debug("SCHEMA_REGISTRY_IDENTITY::::::::"	+ schemaRegistryIdentity);
		logger.debug("NUM_OF_CYCLES::::::::" + numOfCycles); 
		logger.debug("NUM_OF_UPDATES_PER_CYCLE::::::::" + numOfUpdatesPerCycle); 
		logger.debug("DURATION (in Minute)::::::::" + durationInMin); 
		logger.debug("INTERVAL::::::::" + interval); 
		logger.debug("NUM_OF_INTERFACES::::::::" + numOfInteraces); 
		logger.debug("NUM_OF_UPDATES_PER_SEC::::::::" + numOfUpdatesPerSec);  

	}
	public ServiceStatus run() {

		try {
			preExecute();
			execute();
			postExecute();
			return ServiceStatus.SUCCESS;
		}
		catch(Exception e) {
			logger.error(e.getStackTrace().toString());
			logger.error(e.getMessage());
			return ServiceStatus.FAILURE;
		} 
	}   

	protected Schema getSchema(String name) throws IOException, RestClientException {

		SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(schemaRegustryUrl, Integer.parseInt(schemaRegistryIdentity));
		int id = schemaRegistry.getLatestSchemaMetadata(name).getId();
		return schemaRegistry.getByID(id);
	}


	protected Properties getProperties(boolean isAvro) {

		return utils.getProperties(isAvro); 
	}

	protected String getLine(double[] array) {

		StringBuffer output = new StringBuffer();
		for(double d : array ) 
		{
			output.append(seperator).append(d);
		}

		return output.toString();

	}  
	
	/**
	 * Only for testing
	 */

	static SchemaRegistryClient schemaRegistry;
	static ActorMaterializer materializer;
	static ActorSystem system;

	public void setTesting(SchemaRegistryClient schemaRegistry,
			ActorMaterializer materializer, ActorSystem system) {
		this.schemaRegistry = schemaRegistry;
		this.materializer = materializer;
		this.system = system;
	}

	public static List<Pair<GenericRecord, Long>> callConsumersWithAkka(
			String topicName) {

		KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer(
				schemaRegistry);
		keyDeserializer.configure(Collections.singletonMap(
				"schema.registry.url", "http://fake-url"), true);

		final ConsumerSettings<String, Object> consumerSettings = ConsumerSettings
				.create(system, new StringDeserializer(), keyDeserializer)
				.withBootstrapServers("192.168.0.51:9092")
				.withGroupId("group1")
				.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
						"earliest");

		List<Pair<GenericRecord, Long>> consumerRecords = new ArrayList<>();

		Procedure<ConsumerRecord<String, Object>> f = new Procedure<ConsumerRecord<String, Object>>() {

			private static final long serialVersionUID = 1L;

			public void apply(ConsumerRecord<String, Object> param)
					throws Exception {

				System.out.println("Param " + param.value());
				consumerRecords.add(new Pair<GenericRecord, Long>(
						(GenericRecord) param.value(), param.timestamp()));
			}
		};

		System.out.println("=====" + topicName);
		Consumer.committableSource(consumerSettings, Subscriptions.topics(topicName))
				.map(result -> result.record()).runForeach(f, materializer);
		// Wait till the array will be populated from kafka
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
		return consumerRecords;
	}

	protected void callProducerWithAkka(String topic, GenericRecord record) {

		ProducerSettings<String, Object> producerSettings = ProducerSettings
				.create(system, new StringSerializer(),
						new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers(kafkaAddress);

		Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer
				.plainSink(producerSettings);

		ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>(
				topic, record);

		Source.from(Arrays.asList(producerRecord)).to(sink).run(materializer);
	}
}
