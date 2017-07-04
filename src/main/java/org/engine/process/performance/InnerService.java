package org.engine.process.performance;

public abstract class InnerService {

	abstract protected void preExecute() throws Exception;
	abstract protected void postExecute() throws Exception;
	abstract protected ServiceStatus execute() throws Exception;

	public ServiceStatus run() {

		try {

			preExecute();
			execute();
			postExecute();
			return ServiceStatus.SUCCESS;
		}
		catch(Exception e) {

			return ServiceStatus.FAILURE;
		} 
	}

}
