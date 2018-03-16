package btree;

import global.RID;
import global.TID;

public class LeafData extends DataClass {
	
	private TID mytid;
	
	// 
	public String toString() {
		return null;
		
	}
	
	LeafData(TID tid) {
		mytid = new TID(tid.getNumRIDs(),tid.getPosition(),tid.getRecordIDs());
	}
	
	public TID getData() {
		return new TID(mytid.getNumRIDs(),mytid.getPosition(), mytid.getRecordIDs());
		
	}
	
	public void setData(TID tid) {
		mytid = new TID(tid.getNumRIDs(),tid.getPosition(),tid.getRecordIDs());
	}
		
		
		
	
	
	
	
	
	

}
