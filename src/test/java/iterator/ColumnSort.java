package iterator;

import org.junit.Test;

import cmdline.BatchInsert;
import global.Convert;
import global.RID;
import global.SystemDefs;
import heap.Scan;
import heap.Tuple;

public class ColumnSort {
	@Test
	public void columnSortTest() throws Exception {
		String tablename = "Employee";
		int columnNo = 1;

BatchInsert.main(new String[] {"sampledata.txt", "Minibase.min", tablename, "4"} );
		new ColumnarSort("Employee", columnNo);
		//String FileName = tablename + "." + columnNo + ".sort";
		ColumnSortScan scan  = new ColumnSortScan(tablename,(short)columnNo);
		
		RID rid = new RID();
		Tuple tuple = new Tuple();
		tuple = scan.getNext(rid);
		while(rid != null) {
			if(tuple!=null) {
				
				System.out.println(Convert.getStringValue(0, tuple.getTupleByteArray(), 25));
				tuple = scan.getNext(rid);
			}
			
		}
		SystemDefs.JavabaseBM.flushAllPages();
	}
}
