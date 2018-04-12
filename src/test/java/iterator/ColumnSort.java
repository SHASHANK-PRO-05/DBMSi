package iterator;

import org.junit.Test;

import cmdline.BatchInsert;
import global.SystemDefs;
import heap.Scan;

public class ColumnSort {
	@Test
	public void columnSortTest() throws Exception {
		String tablename = "Employee";
		int columnNo = 3;
		BatchInsert.main(new String[] {"sampledata.txt", "Minibase.min", tablename, "4"} );
		ColumnarSort colsort = new ColumnarSort("Employee", columnNo);
		String FileName = tablename + "." + columnNo + ".sort";
		SystemDefs.JavabaseBM.flushAllPages();
	}
}
