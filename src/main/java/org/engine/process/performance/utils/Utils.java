package org.engine.process.performance.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.engine.process.performance.multi.SingleCycle;

public class Utils {

	private static Random random;
	private String seperator = ",";
	private String endl = "\n";
	

	static {
		random = new Random();
	}

	public String randomExternalSystemID() {

		String externalSystemID = "performanceProcess-"+random.nextLong();
		return externalSystemID;

	} 

	public double median(double[] array) {
		int middle = array.length/2;
		if (array.length%2 == 1) {
			return array[middle];
		} else {
			return (array[middle-1] + array[middle]) / 2.0;
		}
	}

	public double mean(double[] array) {
		double sum = 0;
		for (int i = 0; i < array.length; i++) {
			sum += array[i];
		}
		return sum / array.length;
	}

	public double standardDeviation(double[] array) { 

		double sum=0;
		double finalsum = 0;
		double average = 0;

		for( double i : array) {
			finalsum = (sum += i);
		}
		average = finalsum/(array.length);
		//System.out.println("Average: "+ average);

		double sumX=0;
		double finalsumX=0;
		double[] x1_average = new double[array.length+1];
		for (int i = 0; i<array.length; i++){
			double fvalue = (Math.pow((array[i] - average), 2));
			x1_average[i] = fvalue;
			//System.out.println("test: "+ fvalue);
		}

		for(double i : x1_average) {
			finalsumX = (sumX += i);
		}

		Double AverageX = finalsumX/(array.length); 
		double squareRoot = Math.sqrt(AverageX); 

		return squareRoot;
	} 
	
	
 
	public String createCsvFileDataInRows(double[] rowDataToSourceDiffTimeArray,
			double[] sourceToUpdateDiffTimeArray, double[] totalDiffTimeArray,String sourceName) {
	 
		StringBuffer output = new StringBuffer(); 
		output.append("DiffTime between between <"+sourceName+"-row-data> and <"+sourceName+">").append(getLine(rowDataToSourceDiffTimeArray)).append("\n");
		output.append("DiffTime between <"+sourceName+"> and <update>").append(getLine(sourceToUpdateDiffTimeArray)).append("\n");
		output.append("Total DiffTime").append(getLine(totalDiffTimeArray)).append("\n");
		
		return output.toString();
			
 
	}
 	
	public String createCsvFileDataInColumns(double[] rowDataToSourceDiffTimeCreateArray,double[] sourceToUpdateDiffTimeCreateArray, double[] totalDiffTimeCreateArray,
							double[] rowDataToSourceDiffTimeUpdateArray,double[] sourceToUpdateDiffTimeUpdateArray, double[] totalDiffTimeUpdateArray,String sourceName) {
	 
		StringBuffer output = new StringBuffer(); 
		
		
		output.append("CREATE - DiffTime between between <"+sourceName+"-row-data> and <"+sourceName+">").append(seperator);
		output.append("CREATE - DiffTime between <"+sourceName+"> and <update>").append(seperator);
	//	output.append("CREATE - Total DiffTime").append(seperator);
		output.append("UPDATE - DiffTime between between <"+sourceName+"-row-data> and <"+sourceName+">").append(seperator);
		output.append("UPDATE - DiffTime between <"+sourceName+"> and <update>").append("\n");
	//	output.append("UPDATE - Total DiffTime").append("\n");
		int[] maxArray = new int[]{rowDataToSourceDiffTimeCreateArray.length,sourceToUpdateDiffTimeCreateArray.length,
								  rowDataToSourceDiffTimeUpdateArray.length,sourceToUpdateDiffTimeUpdateArray.length};
		 
		Arrays.sort(maxArray);
		int max = maxArray[maxArray.length - 1];
		
		for( int i = 0; i < max; i++) {
			
			if( i < rowDataToSourceDiffTimeCreateArray.length ) {
				output.append(rowDataToSourceDiffTimeCreateArray[i]).append(seperator);
			}
			else {
				output.append("").append(seperator);
			}
			if( i < sourceToUpdateDiffTimeCreateArray.length ) {
				output.append(sourceToUpdateDiffTimeCreateArray[i]).append(seperator);
			}
			else {
				output.append("").append(seperator);
			}
			/*if( i < totalDiffTimeCreateArray.length ) {
				output.append(totalDiffTimeCreateArray[i]).append(seperator);
			}
			else {
				output.append("").append(seperator);
			}*/
			if( i < rowDataToSourceDiffTimeUpdateArray.length ) {
				output.append(rowDataToSourceDiffTimeUpdateArray[i]).append(endl);
			}
			else {
				output.append("").append(seperator);
			}
			if( i < sourceToUpdateDiffTimeUpdateArray.length ) {
				output.append(sourceToUpdateDiffTimeUpdateArray[i]).append(endl);
			}
			else {
				output.append("").append(seperator);
			}
			/*if( i < totalDiffTimeUpdateArray.length ) {
				output.append(totalDiffTimeUpdateArray[i]).append(endl);
			}
			else {
				output.append("").append(endl);
			} */
		} 
		
		return output.toString();
			
 
	}
	 
	private String getLine(double[] array) {
		
		StringBuffer output = new StringBuffer();
		for(double d : array ) 
		{
			output.append(seperator).append(d);
		}
		
		return output.toString();
		
	}
	
	public void printToFile(String output, String fileLocation)   {
		 
		String dateTime = new SimpleDateFormat("yyyyMMdd_HHmm").format(new Date());
		try( FileWriter fw = new FileWriter(fileLocation+"/enginePeformanceResult_"+dateTime+".log"))
		{
			fw.write(output+"\n");	 
		} catch (IOException e) {
			 
			e.printStackTrace();
		}
	}


}
