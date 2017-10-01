package org.engine.process.performance.utils;
 
import java.util.HashSet;
import java.util.List; 
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import joptsimple.internal.Strings;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.engine.process.performance.Main; 
 
public class Utils {

	private static Random random;  
	private static boolean testing = Main.testing;

	static {
		random = new Random();
	}

	public String randomExternalSystemID() {

		String externalSystemID = "performanceProcess-"+random.nextLong();
		return externalSystemID;
	} 

	public double median(double[] array) {
		int middle = array.length/2;
		if (array.length%2 == 1) {
			return array[middle];
		} else {
			return (array[middle-1] + array[middle]) / 2.0;
		}
	}

	public double mean(double[] array) {
		double sum = 0;
		for (int i = 0; i < array.length; i++) {
			sum += array[i];
		}
		return sum / array.length;
	}

	public double standardDeviation(double[] array) { 

		double sum=0;
		double finalsum = 0;
		double average = 0;

		for( double i : array) {
			finalsum = (sum += i);
		}
		average = finalsum/(array.length); 

		double sumX=0;
		double finalsumX=0;
		double[] x1_average = new double[array.length+1];
		for (int i = 0; i<array.length; i++){
			double fvalue = (Math.pow((array[i] - average), 2));
			x1_average[i] = fvalue; 
		}

		for(double i : x1_average) {
			finalsumX = (sumX += i);
		}

		Double AverageX = finalsumX/(array.length); 
		double squareRoot = Math.sqrt(AverageX); 

		return squareRoot;
	}  
	
	/**
	 * The option are - 
	 * Trace < Debug < Info < Warn < Error < Fatal. 
	 * Trace is of the lowest priority and Fatal is having highest priority.  
	 * When we define logger level, anything having higher priority logs are also getting printed
	 * 
	 * @param debugLevel
	 */
	public static void setDebugLevel(Logger logger) {

		String debugLevel = System.getenv("DEBUG_LEVEL");		
		if( Strings.isNullOrEmpty(debugLevel)) {
			debugLevel = "ALL";
		} 

		switch (debugLevel) {

		case "ALL":
			logger.setLevel(Level.ALL);
			break;
		case "DEBUG":
			logger.setLevel(Level.DEBUG);
			break;
		case "ERROR":
			logger.setLevel(Level.ERROR);
			break;
		case "WARNING":
			logger.setLevel(Level.WARN); 
			break;
		default:
			logger.setLevel(Level.ALL);
		} 
	} 
	
	/**
	 * 
	 * Get list of son from entityFamily generic records
	 * and return list of pair <entityId,sourceName>
	 * 
	 * @param entityFamily
	 * @return Set<Pair<String,String>>
	 */
	public Set<Pair<String,String>> getSonsFromRecrod(GenericRecord familyRecord) {		
		
		Set<Pair<String,String>> set = new HashSet<>();	
		@SuppressWarnings("unchecked")
		List<GenericRecord> sonsList =  (List<GenericRecord>)familyRecord.get("sons");  
		for(GenericRecord systemEntity : sonsList) {
			
			GenericRecord entityAttributes = (GenericRecord)systemEntity.get("entityAttributes");
			String externalSystemID = (String) entityAttributes.get("externalSystemID").toString();
			GenericRecord basicAttributes = (GenericRecord)entityAttributes.get("basicAttributes");
			String sourceName = (String) basicAttributes.get("sourceName").toString(); 
			set.add(new Pair<String,String>(externalSystemID,sourceName)); 
		}		
		return set;
	} 
	
	public Properties getProperties(boolean isAvro) {

		String kafkaAddress;
		String schemaRegustryUrl;
		if(testing) {
			kafkaAddress = "192.168.0.51:9092 ";
			schemaRegustryUrl = "http://schema-registry.kafka:8081";
		}
		else {
			kafkaAddress = System.getenv("KAFKA_ADDRESS");
			schemaRegustryUrl = System.getenv("SCHEMA_REGISTRY_ADDRESS"); 		
		}		

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
	
	
	public String convertToString (double num) {
		
		Long d =  new Double(num).longValue();
		return String.valueOf(d); 
	}
}
