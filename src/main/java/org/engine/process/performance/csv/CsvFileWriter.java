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
import org.apache.log4j.Logger;
import org.engine.process.performance.Main;

public class CsvFileWriter {

	private String fileName; 
	private Logger logger = Main.logger;
	
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
		logger.info("Output file is: "+fileName); 
	}

	public void writeCsvFile(List<String> header,Object[] columnsName,List<? extends CsvRecord> data) {

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
	
	private Consumer<CsvRecord> getConsumer(CSVPrinter csvFilePrinter) {

		Consumer<CsvRecord> consumer = new Consumer<CsvRecord> () {

			@Override
			public void accept(CsvRecord record) {

				try {
					csvFilePrinter.printRecord(record.toObjectArray());
				} catch (IOException e) { 

					e.printStackTrace();
				} 
			}}; 

			return consumer;
	} 
}
