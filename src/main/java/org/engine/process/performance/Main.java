package org.engine.process.performance;

import java.io.IOException;

import org.apache.log4j.Logger; 
import org.engine.process.performance.activity.create.CreateActivityMultiMessages;
import org.engine.process.performance.activity.merge.MergeActivityMultiMessages;
import org.engine.process.performance.utils.EventType;
import org.engine.process.performance.utils.InnerService;
import org.engine.process.performance.utils.ServiceStatus;
import org.engine.process.performance.utils.Utils;

/**
 * @author assafsh
 *
 */

public class Main {

	public static boolean testing = false;	
	final static public Logger logger = Logger.getLogger(Main.class);

	static {
		Utils.setDebugLevel(System.getenv("DEBUG_LEVEL"),logger);
	}

	public static void main(String[] args) throws InterruptedException,
			IOException {

		String printToFile = System.getenv("PRINT_TO_FILE");
		String fileLocation = System.getenv("FILE_LOCATION");
		String secToDelay = System.getenv("SEC_TO_DELAY"); 
		String debugLevel = System.getenv("DEBUG_LEVEL");
		String activity = System.getenv("ACTIVITY");
		
		if(testing) {			
			printToFile = "false";
			activity = "MERGE";
		}

		logger.debug("PRINT_TO_FILE::::::::" + printToFile);
		logger.debug("FILE_LOCATION::::::::" + fileLocation);
		logger.debug("SEC_TO_DELAY::::::::" + secToDelay);
		logger.debug("DEBUG_LEVEL::::::::" + debugLevel);
		logger.debug("ACTIVITY::::::::" + activity);

		Thread.sleep((secToDelay == null ? 0 : Long.parseLong(secToDelay)) * 1000);

		InnerService service = null;

		switch (EventType.valueOf(activity)) {

		case CREATE:
			service = new CreateActivityMultiMessages();
			break;
		case MERGE:
			service = new MergeActivityMultiMessages();
			break;
		case SPLIT:

			break;
		default:
			logger.error("Activity " + activity + " is not supported");
			System.exit(-1);
		}

		ServiceStatus status = service.run();

		if (status != ServiceStatus.SUCCESS) {
			logger.error(status.getMessage());
			System.exit(-1);
		}

		logger.info(service.getOutput());

		if (printToFile.equalsIgnoreCase("true")) {

			service.printOutputToFile(fileLocation);
		}

		logger.info("END!");
	}
}
