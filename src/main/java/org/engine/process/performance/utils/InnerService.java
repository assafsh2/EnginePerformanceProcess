package org.engine.process.performance.utils;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.engine.process.performance.Main;
import org.engine.process.performance.activity.merge.MergeActivityMultiMessages;


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

	public InnerService() {

		utils = new Utils(); 
		cyclesList = new ArrayList<>();

		if(Main.testing) {
			kafkaAddress = "192.168.0.51:9092 ";
			schemaRegustryUrl = "http://schema-registry.kafka:8081";
			schemaRegistryIdentity = "2021";
			sourceName = "source1"; 		
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


}
