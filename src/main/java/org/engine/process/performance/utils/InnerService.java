package org.engine.process.performance.utils;

import org.apache.log4j.Logger;
import org.engine.process.performance.Main;
import org.engine.process.performance.ServiceStatus;
 

public abstract class InnerService {

	abstract protected void preExecute() throws Exception;
	abstract protected void postExecute() throws Exception;
	abstract protected ServiceStatus execute() throws Exception;
	abstract public String getOutput();
	abstract public void printOutputToFile(String fileLocation);
	protected Logger logger = Main.logger;
	
	protected Utils utils = new Utils();

	public ServiceStatus run() {

		try {

			preExecute();
			execute();
			postExecute();
			return ServiceStatus.SUCCESS;
		}
		catch(Exception e) {
			logger.error(e.getStackTrace());
			logger.error(e.getMessage());
			return ServiceStatus.FAILURE;
		} 
	}  
}
