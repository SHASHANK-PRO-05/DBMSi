package iterator;

import java.io.IOException;

import org.junit.Test;

import cmdline.BatchInsert;
import columnar.ColumnarSortTupleScan;
import global.Convert;
import global.IntegerValue;
import global.RID;
import global.StringValue;
import global.SystemDefs;
import global.ValueClass;
import heap.Scan;
import heap.Tuple;

public class ColumnSort {
	@Test
	public void columnSortTest() throws Exception {
		String tablename = "Employee";
		int columnNo = 1;

		BatchInsert.main(new String[] { "sampledata.txt", "Minibase.min", tablename, "4" });
		new ColumnarSort("Employee", columnNo, "ASC");
		
		ColumnarSortTupleScan scan = new ColumnarSortTupleScan(tablename, (short) columnNo, "ASC");

		Tuple tuple = scan.getNext();
		while (tuple != null) {
			tuple = scan.getNext();
		}

		SystemDefs.JavabaseBM.flushAllPages();
	}

	ValueClass getValuefromSortByte(byte[] record) throws IOException {
		// int size = sortedColumnType.getSize();
		ValueClass val = null;
		// if (sortedColumnType.getAttrType() == 1) {
		val = new IntegerValue(Convert.getIntValue(0, record));
		// } else if (sortedColumnType.getAttrType() == 0) {
		// val = new StringValue(Convert.getStringValue(0, record, size));

		// }
		return val;
	}
}