package columnar;

import global.ValueClass;

public class IndexInfo {
	
	private int columnNumber;
	private ValueClass value;
	public int getColumnNumber() {
		return columnNumber;
	}
	public void setColumnNumber(int columnNumber) {
		this.columnNumber = columnNumber;
	}
	public ValueClass getValue() {
		return value;
	}
	public void setValue(ValueClass value) {
		this.value = value;
	}
	

}
