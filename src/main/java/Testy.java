import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.ColumnarSortTupleScan;
import global.*;
import heap.Tuple;
import iterator.ColumnarSort;

import java.io.IOException;
import java.util.ArrayList;

public class Testy {
    public static void main(String[] args) throws Exception {
        Testy test = new Testy();
        test.columnSortTest();
    }

    public void columnSortTest() throws Exception {
        String tablename = "Employee";
        int columnNo = 2;
        SystemDefs systemDefs = new SystemDefs("Minibase.min", 0, 4000, "LRU");
        //BatchInsert.main(new String[]{"smal.txt", "Minibase.min", tablename, "4", "400"});
        new ColumnarSort("Employee", columnNo, "DSC");

        ColumnarSortTupleScan scan = new ColumnarSortTupleScan(tablename, (short) columnNo, "DSC");

        ColumnarFile columnfile = new ColumnarFile("Employee");
        AttrType[] attrtypes = columnfile.getColumnarHeader().getColumns();
        int counter = 1;
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
        ValueClass val = null;
        val = new IntegerValue(Convert.getIntValue(0, record));
        return val;
    }
}
