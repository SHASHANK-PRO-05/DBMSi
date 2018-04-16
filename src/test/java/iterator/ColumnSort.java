package iterator;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import cmdline.BatchInsert;
import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.ColumnarSortTupleScan;
import global.AttrType;
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

		ColumnarFile columnfile = new ColumnarFile("Employee");
		AttrType[] attrtypes = columnfile.getColumnarHeader().getColumns();
		int counter = 0;
		ByteToTuple byteToTuple = new ByteToTuple(attrtypes);
		Tuple tuple = scan.getNext();
		while (tuple != null) {
			ArrayList<byte[]> tuples = byteToTuple.setTupleBytes(tuple.getTupleByteArray());
			String ans1 = counter + "";
			System.out.print(ans1);
			int temp1 = 25 - ans1.length();
			for (int j = 0; j < temp1; j++)
				System.out.print(" ");

			counter++;
			for (int i = 0; i < attrtypes.length; i++) {
				if (attrtypes[i].getAttrType() == AttrType.attrString) {
					String ans = Convert.getStringValue(0, tuples.get(i), attrtypes[i].getSize());
					System.out.print(ans);
					int temp = 25 - ans.length();
					for (int j = 0; j < temp; j++)
						System.out.print(" ");
				} else {
					int ans = Convert.getIntValue(0, tuples.get(i));
					System.out.print(ans);
					int temp = 25 - (ans + "").length();
					for (int j = 0; j < temp; j++)
						System.out.print(" ");
				}
			}
			System.out.println();
			tuple = scan.getNext();
		}
		
		scan.closeScan();
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