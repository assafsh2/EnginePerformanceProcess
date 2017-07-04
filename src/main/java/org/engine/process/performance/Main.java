package org.engine.process.performance;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author assafsh
 *
 */

public class Main {

	public static void main(String[] args) throws InterruptedException, IOException {
		 
		String kafkaAddress = System.getenv("KAFKA_ADDRESS");
		String schemaRegustryUrl = System.getenv("SCHEMA_REGISTRY_ADDRESS");
		String schemaRegustryIdentity = System.getenv("SCHEMA_REGISTRY_IDENTITY");
		String sourceName = System.getenv("SOURCE_NAME");
		String printToFile = System.getenv("PRINT_TO_FILE");
		String fileLocation = System.getenv("FILE_LOCATION");		
		String secToDelay = System.getenv("SEC_TO_DELAY");
				
		System.out.println("KAFKA_ADDRESS::::::::" + kafkaAddress);
		System.out.println("SCHEMA_REGISTRY_ADDRESS::::::::" + schemaRegustryUrl); 
		System.out.println("SCHEMA_REGISTRY_IDENTITY::::::::" + schemaRegustryIdentity);
		System.out.println("SOURCE_NAME::::::::" + sourceName);
		System.out.println("PRINT_TO_FILE::::::::" + printToFile);
		System.out.println("FILE_LOCATION::::::::" + fileLocation);
		System.out.println("SEC_TO_DELAY::::::::" + secToDelay);
		
		Thread.sleep((secToDelay == null ? 0 : Long.parseLong(secToDelay))*1000);
		
		EnginePerformance enginePerformance = new EnginePerformance(kafkaAddress,schemaRegustryUrl,schemaRegustryIdentity,sourceName);
		ServiceStatus status = enginePerformance.run();
		System.out.println(status.getMessage());
		
		if(status != ServiceStatus.SUCCESS)
			System.exit(-1);
		
		if(printToFile.equalsIgnoreCase("true")) {
			
			printToFile(enginePerformance.getOutput(),fileLocation);
			
		}
		else {
			System.out.println(enginePerformance.getOutput()[0]);
			System.out.println(enginePerformance.getOutput()[1]);
		}
	}
	
	public static void printToFile(String[] output, String fileLocation) throws IOException {
 
		String dateTime = new SimpleDateFormat("yyyyMMdd_HHmm").format(new Date());
		try( FileWriter fw = new FileWriter(fileLocation+"/enginePeformanceResult_"+dateTime+".log"))
		{
			fw.write(output[0]+"\n");
			fw.write(output[1]);
		}
	}

	 

}
