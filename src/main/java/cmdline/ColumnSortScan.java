package cmdline;

import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.ColumnarSortTupleScan;
import global.AttrType;
import global.Convert;
import global.SystemDefs;
import heap.Heapfile;
import heap.Tuple;
import iterator.ColumnarSort;

import java.util.ArrayList;

public class ColumnSortScan {
    public ColumnarFile columnarFile;
    public ColumnarSortTupleScan columnarSortTupleScan;
    public String relName;
    public String colNum;
    public String order;
    public double startTime;

    public ColumnSortScan() {

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("Usage: ColumnSortScan COLUMNDBNAME COLUMNARFILENAME COLUMNNAME ORDER BUFFERSIZE");
        }
        ColumnSortScan columnSortScan = new ColumnSortScan();
        columnSortScan.initFromArgs(args);
        columnSortScan.begin();
    }

    public void begin() throws Exception {
        ColumnarFile columnfile = new ColumnarFile(relName);
        AttrType[] attrtypes = columnfile.getColumnarHeader().getColumns();
        String ans2 = "Count";
        System.out.print(ans2);
        int temp2 = 25 - ans2.length();
        for (int j = 0; j < temp2; j++)
            System.out.print(" ");
        for (int i = 0; i < attrtypes.length; i++) {
            String ans = attrtypes[i].getAttrName();
            System.out.print(ans);
            int temp = 25 - ans.length();
            for (int j = 0; j < temp; j++)
                System.out.print(" ");

        }
        System.out.println();
        int counter = 1;
        ByteToTuple byteToTuple = new ByteToTuple(attrtypes);
        Tuple tuple = columnarSortTupleScan.getNext();
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
            tuple = columnarSortTupleScan.getNext();
        }

        columnarSortTupleScan.closeScan();
        Heapfile heapfile = new Heapfile(relName + ".s" + colNum + order);
        heapfile.deleteFile();
        double endTime = System.currentTimeMillis();
        double duration = (endTime - startTime);
        System.out.println("Time taken (Seconds)" + duration / 1000);
        System.out.println("Tuples in the table now:" + columnarFile.getTupleCount());
        System.out.println("Write count: " + SystemDefs.pCounter.getwCounter());
        System.out.println("Read count: " + SystemDefs.pCounter.getrCounter());
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
    }

    public void initFromArgs(String[] args) throws Exception {
        String dbName = args[0];
        String tableName = args[1];
        relName = args[1];
        String columnName = args[2];
        order = args[3];
        int bufferSize = Integer.parseInt(args[4]);
        SystemDefs systemDefs = new SystemDefs(dbName,
                0, bufferSize, "LRU");
        columnarFile = new ColumnarFile(tableName);
        AttrType[] attrTypes = columnarFile.getColumnarHeader().getColumns();
        int columnNumber = -1;
        for (AttrType attrType : attrTypes) {
            if (attrType.getAttrName().equals(columnName)) {
                columnNumber = attrType.getColumnId();
                break;
            }
        }
        if (columnNumber == -1)
            throw new Exception("Invalid column name");
        colNum = columnNumber + "";
        startTime = System.currentTimeMillis();
        new ColumnarSort(tableName, columnNumber, order);
        columnarSortTupleScan = new ColumnarSortTupleScan(tableName, (short) columnNumber, order);
    }
}
