package iterator;

import global.StringValue;
import global.ValueClass;

public class SortInfo implements Comparable<SortInfo>{
	ValueClass val;
	int position;
	
	public SortInfo(ValueClass value, int count) {
		val = value;
		position = count;
	}
	public ValueClass getVal() {
		return val;
	}
	public void setVal(ValueClass val) {
		this.val = val;
	}
	public int getPosition() {
		return position;
	}
	public void setPosition(int position) {
		this.position = position;
	}
	
	public int compareTo(SortInfo o) {
		if(o.val.getValueType() == 0) {
			if(o.val.getValueType() == val.getValueType()) {
				return this.val.compare(o.val);
			}
		}
		if(o.val.getValueType() == 1) {
			if(o.val.getValueType() == val.getValueType()) {
				return this.val.compare(o.val);
			}
		}
		return -1;
	}

}
