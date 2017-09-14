package org.engine.process.performance.csv;

public class TopicTimeData <T> {
	
	private T rawDataToSource;
	private T sourceToUpdate;	
	
	public TopicTimeData(T rawDataToSource,T sourceToUpdate) {
		
		this.rawDataToSource = rawDataToSource;
		this.sourceToUpdate = sourceToUpdate;
	}
	
	public TopicTimeData() {}

	public T getRawDataToSource() {
		return rawDataToSource;
	}

	public void setRawDataToSource(T rawDataToSource) {
		this.rawDataToSource = rawDataToSource;
	}

	public T getSourceToUpdate() {
		return sourceToUpdate;
	}

	public void setSourceToUpdate(T sourceToUpdate) {
		this.sourceToUpdate = sourceToUpdate;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((rawDataToSource == null) ? 0 : rawDataToSource.hashCode());
		result = prime * result
				+ ((sourceToUpdate == null) ? 0 : sourceToUpdate.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TopicTimeData<T> other = (TopicTimeData<T>) obj;
		if (rawDataToSource == null) {
			if (other.rawDataToSource != null)
				return false;
		} else if (!rawDataToSource.equals(other.rawDataToSource))
			return false;
		if (sourceToUpdate == null) {
			if (other.sourceToUpdate != null)
				return false;
		} else if (!sourceToUpdate.equals(other.sourceToUpdate))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TopicData [rawDataToSource=" + rawDataToSource
				+ ", sourceToUpdate=" + sourceToUpdate + "]";
	} 
	
	public Object[] toObjectArray() {
		
		Object[] obj = {rawDataToSource, sourceToUpdate};
		return obj;
	}
}
