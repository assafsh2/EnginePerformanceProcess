package org.engine.process.performance.utils;

import java.util.Random;

public class Utils {

	private static Random random;

	static {
		random = new Random();
	}

	public String randomExternalSystemID() {

		String externalSystemID = "performanceProcess-"+random.nextLong();
		return externalSystemID;

	} 

	public double median(long[] array) {
		int middle = array.length/2;
		if (array.length%2 == 1) {
			return array[middle];
		} else {
			return (array[middle-1] + array[middle]) / 2.0;
		}
	}

	public double mean(long[] array) {
		double sum = 0;
		for (int i = 0; i < array.length; i++) {
			sum += array[i];
		}
		return sum / array.length;
	}

	public double standardDeviation(long[] array) { 

		double sum=0;
		double finalsum = 0;
		double average = 0;

		for( long i : array) {
			finalsum = (sum += i);
		}
		System.out.println("Average: "+ finalsum/(array.length));

		double sumX=0;
		double finalsumX=0;
		double[] x1_average = new double[array.length+1];
		for (int i = 0; i<array.length; i++){
			double fvalue = (Math.pow((array[i] - average), 2));
			x1_average[i] = fvalue;
			System.out.println("test: "+ fvalue);
		}

		for(double i : x1_average) {
			finalsumX = (sumX += i);
		}

		Double AverageX = finalsumX/(array.length); 
		double squareRoot = Math.sqrt(AverageX); 

		return squareRoot;
	} 

}
