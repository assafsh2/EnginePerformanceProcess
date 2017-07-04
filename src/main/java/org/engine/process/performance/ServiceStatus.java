package org.engine.process.performance;

public enum ServiceStatus {
	 
	SUCCESS("The process ended successfully"),
	FAILURE("The process ended with failure");
	
	private String message;
	
	private ServiceStatus(String message) {
		
		this.message = message;
	} 
	
	public String getMessage() {
		
		return this.message;
	} 
}
