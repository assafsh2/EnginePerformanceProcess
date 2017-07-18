package org.engine.process.performance.utils;

import java.util.Random;

public class Utils {
	
	private static Random random;
	
	static {
		random = new Random();
	}
	
	public String randomExternalSystemID() {

		String externalSystemID = "performanceProcess-"+random.nextInt(10000);
		return externalSystemID;
		
	} 

}
