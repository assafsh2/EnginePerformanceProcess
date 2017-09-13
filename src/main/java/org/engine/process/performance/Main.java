package org.engine.process.performance;
 
import java.io.IOException; 

import joptsimple.internal.Strings;

import org.apache.log4j.Level; 
import org.apache.log4j.Logger; 
import org.engine.process.performance.multi.EngingPerformanceMultiCycles; 
import org.engine.process.performance.utils.InnerService;


/**
 * @author assafsh
 *
 */

public class Main {

	final static public Logger logger = Logger.getLogger(Main.class);
	
	static {
		
		setDebugLevel(System.getenv("DEBUG_LEVEL"));
	}

	public static void main(String[] args) throws InterruptedException, IOException {


		String kafkaAddress = System.getenv("KAFKA_ADDRESS");
		String schemaRegistryUrl = System.getenv("SCHEMA_REGISTRY_ADDRESS");
		String schemaRegistryIdentity = System.getenv("SCHEMA_REGISTRY_IDENTITY");
		String sourceName = System.getenv("SOURCE_NAME");
		String printToFile = System.getenv("PRINT_TO_FILE");
		String fileLocation = System.getenv("FILE_LOCATION");		
		String secToDelay = System.getenv("SEC_TO_DELAY");
		String multiMessages = System.getenv("MULTI_MESSAGES");
		String debugLevel = System.getenv("DEBUG_LEVEL");

		logger.debug("KAFKA_ADDRESS::::::::" + kafkaAddress);
		logger.debug("SCHEMA_REGISTRY_ADDRESS::::::::" + schemaRegistryUrl); 
		logger.debug("SCHEMA_REGISTRY_IDENTITY::::::::" + schemaRegistryIdentity);
		logger.debug("SOURCE_NAME::::::::" + sourceName);
		logger.debug("PRINT_TO_FILE::::::::" + printToFile);
		logger.debug("FILE_LOCATION::::::::" + fileLocation);
		logger.debug("SEC_TO_DELAY::::::::" + secToDelay);
		logger.debug("MULTI_MESSAGES::::::::" + multiMessages); 
		logger.debug("MULTI_MESSAGES::::::::" + debugLevel); 

		Thread.sleep((secToDelay == null ? 0 : Long.parseLong(secToDelay))*1000);

		setDebugLevel(debugLevel);
		InnerService service;

		if(multiMessages.equalsIgnoreCase("true")) {
			service = new EngingPerformanceMultiCycles(kafkaAddress,schemaRegistryUrl,schemaRegistryIdentity,sourceName);	
		}	
		else  {
			service = new EnginePerformanceSingleMessage(kafkaAddress,schemaRegistryUrl,schemaRegistryIdentity,sourceName);
		} 

		ServiceStatus status = service.run(); 

		if(status != ServiceStatus.SUCCESS) {	
			logger.error(status.getMessage());
			System.exit(-1);
		}		 

		System.out.println(service.getOutput());

		if(printToFile.equalsIgnoreCase("true")) {

			service.printOutputToFile(fileLocation);

		} 

		logger.debug("END!");
	} 

	private static void setDebugLevel(String debugLevel) {


		if( Strings.isNullOrEmpty(debugLevel)) {
			debugLevel = "ERROR";
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
}
