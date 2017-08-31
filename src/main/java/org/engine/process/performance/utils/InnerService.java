package org.engine.process.performance.utils;

import org.engine.process.performance.ServiceStatus;

public abstract class InnerService {

	abstract protected void preExecute() throws Exception;
	abstract protected void postExecute() throws Exception;
	abstract protected ServiceStatus execute() throws Exception;
	abstract public String getOutput();
	abstract public String getOutputToFile();
	
	protected Utils utils = new Utils();

	public ServiceStatus run() {

		try {

			preExecute();
			execute();
			postExecute();
			return ServiceStatus.SUCCESS;
		}
		catch(Exception e) {
			System.out.println(e.getStackTrace());
			System.out.println(e.getMessage());
			return ServiceStatus.FAILURE;
		} 
	}

}
