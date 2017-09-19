package org.engine.process.performance.csv;

public class CsvRecordForMerge implements CsvRecord {
	
	private String mergeToSource;

	public CsvRecordForMerge(String mergeToSource) {
		this.mergeToSource = mergeToSource;
	}

	@Override
	public Object[] toObjectArray() {
		
		Object[] obj = {mergeToSource};
		return obj;
	}

	public void setMergeToSource(String mergeToSource) {
		this.mergeToSource = mergeToSource;
	} 
}
