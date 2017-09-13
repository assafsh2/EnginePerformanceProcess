package org.engine.process.performance.csv;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class CsvFileWriter {

	private String fileName; 
	
	public CsvFileWriter(String fileLocation) {
		
		if(fileLocation == null || fileLocation.isEmpty()) {
			fileLocation = System.getenv("HOME");
		} 
		
		File dir = new File(fileLocation);
		if( !dir.exists()) {
			dir.mkdir();
		}  
		
		String dateTime = new SimpleDateFormat("yyyyMMdd_HHmm").format(new Date());
		fileName = fileLocation+"/enginePeformanceResult_"+dateTime+".csv";
		System.out.println("Output file is: "+fileName); 

	}

	public void writeCsvFile(List<String> header,Object[] columnsName,List<CsvRecordForCreate> data) {

		CSVFormat csvFileFormat =  CSVFormat.EXCEL;
		
		try(FileWriter fileWriter = new FileWriter(fileName);
			CSVPrinter	csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat)) 		
		{ 
			csvFilePrinter.printRecords(header); 			
			csvFilePrinter.printRecord(columnsName); 
			data.forEach(getConsumer(csvFilePrinter));
			
		} catch (IOException e) {

			e.printStackTrace();
		}
	}  
	
	private Consumer<CsvRecordForCreate> getConsumer(CSVPrinter csvFilePrinter) {

		Consumer<CsvRecordForCreate> consumer = new Consumer<CsvRecordForCreate> () {

			@Override
			public void accept(CsvRecordForCreate record) {

				try {
					csvFilePrinter.printRecord(record.toObjectArray());
				} catch (IOException e) { 

					e.printStackTrace();
				} 
			}}; 

			return consumer;
	} 
}
