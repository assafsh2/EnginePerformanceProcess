package org.engine.process.performance;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.engine.process.performance.multi.EngingPerformanceMultiPeriods;
import org.engine.process.performance.multi.HandlePerformanceMessages;
import org.engine.process.performance.utils.InnerService;

/**
 * @author assafsh
 *
 */

public class Main {

	public static void main(String[] args) throws InterruptedException, IOException {
		 
		String kafkaAddress = System.getenv("KAFKA_ADDRESS");
		String schemaRegistryUrl = System.getenv("SCHEMA_REGISTRY_ADDRESS");
		String schemaRegistryIdentity = System.getenv("SCHEMA_REGISTRY_IDENTITY");
		String sourceName = System.getenv("SOURCE_NAME");
		String printToFile = System.getenv("PRINT_TO_FILE");
		String fileLocation = System.getenv("FILE_LOCATION");		
		String secToDelay = System.getenv("SEC_TO_DELAY"); 
		String startFromBeginning = System.getenv("START_FROM_BEGINNING"); 
		String multiMessages = System.getenv("MULTI_MESSAGES");
				 
		System.out.println("KAFKA_ADDRESS::::::::" + kafkaAddress);
		System.out.println("SCHEMA_REGISTRY_ADDRESS::::::::" + schemaRegistryUrl); 
		System.out.println("SCHEMA_REGISTRY_IDENTITY::::::::" + schemaRegistryIdentity);
		System.out.println("SOURCE_NAME::::::::" + sourceName);
		System.out.println("PRINT_TO_FILE::::::::" + printToFile);
		System.out.println("FILE_LOCATION::::::::" + fileLocation);
		System.out.println("SEC_TO_DELAY::::::::" + secToDelay); 
		System.out.println("START_FROM_BEGINNING::::::::" + startFromBeginning); 
		System.out.println("MULTI_MESSAGES::::::::" + multiMessages); 		
		
		Thread.sleep((secToDelay == null ? 0 : Long.parseLong(secToDelay))*1000);
		
		InnerService service;
		
		if(multiMessages.equalsIgnoreCase("true")) {
			service = new EngingPerformanceMultiPeriods(kafkaAddress,schemaRegistryUrl,schemaRegistryIdentity,sourceName);	
		}	
		else if(startFromBeginning.equalsIgnoreCase("true")) {
			service = new EnginePerformanceFromBeginning(kafkaAddress,schemaRegistryUrl,schemaRegistryIdentity,sourceName);
		}
		else {
			service = new EnginePerformance(kafkaAddress,schemaRegistryUrl,schemaRegistryIdentity,sourceName);
		}
			 
 
		ServiceStatus status = service.run();
		System.out.println(status.getMessage());
		
		if(status != ServiceStatus.SUCCESS)
			System.exit(-1);
		
		if(printToFile.equalsIgnoreCase("true")) {
			
			printToFile(service.getOutput(),fileLocation);
			
		}
		else { 
				System.out.println(service.getOutput());	 
		}
	}
	
	public static void printToFile(String output, String fileLocation) throws IOException {
 
		String dateTime = new SimpleDateFormat("yyyyMMdd_HHmm").format(new Date());
		try( FileWriter fw = new FileWriter(fileLocation+"/enginePeformanceResult_"+dateTime+".log"))
		{
			fw.write(output+"\n");	 
		}
	}

	 

}
