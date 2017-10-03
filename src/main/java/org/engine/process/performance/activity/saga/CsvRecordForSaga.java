package org.engine.process.performance.activity.saga;

import org.engine.process.performance.csv.CsvRecord;

public class CsvRecordForSaga implements CsvRecord {
	
	private String mergeToUpdate;
	private String splitToUpdate;

	public CsvRecordForSaga(String mergeToUpdate,String splitToUpdate) {
		this.mergeToUpdate = mergeToUpdate;
		this.splitToUpdate = splitToUpdate;
	}

	@Override
	public Object[] toObjectArray() {		
		Object[] obj = {mergeToUpdate,splitToUpdate};
		return obj;
	}

	public void setMergeToSource(String mergeToUpdate) {
		this.mergeToUpdate = mergeToUpdate;
	} 
	
	public void setSplitToSource(String splitToUpdate) {
		this.splitToUpdate = splitToUpdate;
	} 
}
