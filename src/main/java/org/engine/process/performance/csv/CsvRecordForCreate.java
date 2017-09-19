package org.engine.process.performance.csv;

/**
 * @author assafsh
 * 
 * Arrange the data for one record in the csv output gile
 * Create\Update message
 *
 */
public class CsvRecordForCreate implements CsvRecord {
	
	private TopicTimeData<String> createAction;
	private TopicTimeData<String> updateAction;	
	
	public CsvRecordForCreate(TopicTimeData<String> createAction, TopicTimeData<String> updateAction) {
		 
		this.createAction = createAction;
		this.updateAction = updateAction;
	}
	public TopicTimeData<String> getCreateAction() {
		return createAction;
	}
	public void setCreateAction(TopicTimeData<String> createAction) {
		this.createAction = createAction;
	}
	public TopicTimeData<String> getUpdateAction() {
		return updateAction;
	}
	public void setUpdateAction(TopicTimeData<String> updateAction) {
		this.updateAction = updateAction;
	}
	
	@Override
	public String toString() {
		return "CsvRecordForCreate [createAction=" + createAction
				+ ", updateAction=" + updateAction + "]";
	}
	
	@Override
	public Object[] toObjectArray() {
		
		Object[] obj = new Object[createAction.toObjectArray().length+updateAction.toObjectArray().length];
		int i = 0; 
		for(Object o : createAction.toObjectArray()) {
			obj[i++] = o;		
		}
		for(Object o : updateAction.toObjectArray()) {
			obj[i++] = o;
		}
		return obj;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((createAction == null) ? 0 : createAction.hashCode());
		result = prime * result
				+ ((updateAction == null) ? 0 : updateAction.hashCode());
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
		CsvRecordForCreate other = (CsvRecordForCreate) obj;
		if (createAction == null) {
			if (other.createAction != null)
				return false;
		} else if (!createAction.equals(other.createAction))
			return false;
		if (updateAction == null) {
			if (other.updateAction != null)
				return false;
		} else if (!updateAction.equals(other.updateAction))
			return false;
		return true;
	}  
}
